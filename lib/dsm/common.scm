(define-module dsm.common
  (extend dsm.dsm)
  (use text.tree)
  (use srfi-10)
  (use srfi-13)
  (use gauche.net)
  (use gauche.charconv)
  (use gauche.vm.debugger)
  (use marshal)
  (export dsmp-request dsmp-response
          version-of encoding-of size-of)
  )
(select-module dsm.common)

(define dsmp-version 1)
(define dsmp-delimiter ";")

(define-class <dsmp-error> ()
  ((message :init-keyword :message :accessor message-of)
   (stack-trace :init-keyword :stack-trace :accessor stack-trace-of)))

(define-method initialize ((self <dsmp-error>) args)
  (next-method)
  (let-keywords* args ((stack-trace-source #f))
    (if stack-trace-source
        (set! (stack-trace-of self)
              (make-stack-trace-from-source stack-trace-source)))))

(define (make-stack-trace-from-source src)
  (map (lambda (s)
         (if (car s)
             (pair-attribute-set! (cadr s) 'source-info (car s)))
         (cdr s))
       src))

(define (make-stack-trace-source st)
  (map (lambda (s)
         (let ((info (if (pair? (car s))
                         (pair-attribute-get (car s) 'source-info #f)
                         #f)))
           (cons (if info
                     (cons (format "~a:~a"
                                   (sys-gethostname)
                                   (car info))
                           (cdr info))
                     #f)
                 s)))
       st))

(define-method write-object ((self <dsmp-error>) out)
  (format out "#,(<dsmp-error> :message ~s :stack-trace-source ~a)"
          (message-of self)
          (marshal (make-marshal-table)
                   (make-stack-trace-source (stack-trace-of self)))))

(define-reader-ctor '<dsmp-error>
  (lambda args
    (apply make <dsmp-error> args)))

(define-method marshalizable? ((self <dsmp-error>))
  #t)

(define (dsmp-error? obj)
  (is-a? obj <dsmp-error>))

(define-class <dsmp-header> ()
  ((version :init-keyword :version :accessor version-of)
   (encoding :init-keyword :encoding :accessor encoding-of)
   (size :init-keyword :size :accessor size-of)
   (command :init-keyword :command :accessor command-of)))

(define (make-dsmp-header alist)
  (define (set-dsmp-header-slot! header key value)
    (cond ((rxmatch #/^v/ key)
           (slot-set! header 'version (x->number value)))
          ((rxmatch #/^e/ key)
           (slot-set! header 'encoding value))
          ((rxmatch #/^c/ key)
           (slot-set! header 'command value))
          ((rxmatch #/^s/ key)
           (slot-set! header 'size (x->number value)))))

  (let ((dsmp-header (make <dsmp-header>)))
    (for-each (lambda (elem)
                (set-dsmp-header-slot! dsmp-header
                                       (x->string (car elem)) (cdr elem)))
              alist)
    dsmp-header))

(define (x->dsmp-header table obj . keywords)
  (apply make-dsmp-header-from-string (marshal table obj) keywords))

(define (make-dsmp-header-from-string str . keywords)
  (make-dsmp-header`(("v" . ,dsmp-version)
                     ("e" . ,(ces-guess-from-string str "*JP"))
                     ("s" . ,(string-size str))
                     ("c" . ,(get-keyword :command keywords)))))

(define (dsmp-header->string header)
  (string-join (list #`"v=,(version-of header)"
                     #`"e=,(encoding-of header)"
                     #`"s=,(size-of header)"
                     #`"c=,(command-of header)")
               dsmp-delimiter))

(define (x->dsmp-header->string table obj . keywords)
  (dsmp-header->string (apply x->dsmp-header table obj keywords)))

(define (parse-dsmp-header str)
  (make-dsmp-header (map (lambda (elem)
                          (let ((splited-elem (string-split elem "=")))
                            (cons (car splited-elem)
                                  (cadr splited-elem))))
                        (string-split (string-trim-right str)
                                      dsmp-delimiter))))

(define (dsmp-write header body output)
;;  (p (list "write" header body))
  (display header output)
  (display "\n" output)
  (display body output)
  (flush output))

(define (read-dsmp input . keywords)
  (let-keywords* keywords ((eof-handler (lambda () "Got eof")))
    ;; (p (list "reading..."))
    (let* ((header (read-dsmp-header input eof-handler))
           (body (read-dsmp-body header input eof-handler)))
      ;; (p (list "read" (dsmp-header->string header) body))
      (values header body))))
  
(define (read-dsmp-header input eof-handler)
  (let ((header (read-line input)))
;;    (p header)
    (if (eof-object? header)
        (eof-handler)
        (parse-dsmp-header header))))

(define (read-dsmp-body header input eof-handler)
;;  (p header)
  (let ((body (read-block (size-of header) input)))
;;    (p body)
    (if (eof-object? body)
        (eof-handler)
        (read-from-string (ces-convert body
                                       (encoding-of header)
                                       (gauche-character-encoding))))))

(define (need-remote-eval? obj table)
  (and (reference-object? obj)
       (not (using-same-table? table obj))))

(define (dsmp-request marshaled-obj table in out . keywords)
  (define (show-error message stack-trace)
    (display (format "*** ERROR: ~s\n" message)
             (current-error-port))
    (with-module gauche.vm.debugger
      (debug-print-stack stack-trace
                         *stack-show-depth*)))
  (define (report-if-error obj)
    (if (dsmp-error? obj)
        (show-error (message-of obj)
                    (stack-trace-of obj))
        obj))
  
  (let-keywords* keywords ((command "get")
                           (get-handler report-if-error)
                           (post-handler report-if-error)
                           (eof-handler (lambda ()
                                          (print "Got eof from server"))))
    (define (dsmp-handler)
      (dsmp-write (dsmp-header->string
                   (make-dsmp-header-from-string marshaled-obj
                                                 :command command))
                  marshaled-obj
                  out)
      (receive (header body)
          (read-dsmp in :eof-handler eof-handler)
        (handle-response header body)))

    (define (handle-response header body)
      (let ((obj (handle-dsmp-body (command-of header)
                                   body table
                                   in out
                                   :get-handler get-handler
                                   :response-handler response-handler
                                   :post-handler post-handler)))
        (cond ((string=? "eval" (command-of header))
               (let ((marshaled-obj (marshal table obj)))
                 (dsmp-request marshaled-obj table
                               in out
                               :command "response"
                               :get-handler get-handler
                               :post-handler post-handler
                               :eof-handler eof-handler)))
              (else obj))))
    
    (define (response-handler obj)
      (if (need-remote-eval? obj table)
          (lambda arg
            (eval-in-remote obj arg table in out
                            :get-handler get-handler
                            :post-handler post-handler
                            :eof-handler eof-handler))
          (post-handler obj)))

    (dsmp-handler)))

(define (eval-in-remote obj arg table in out . keywords)
  (apply dsmp-request
         (marshal table (cons obj arg))
         table in out
         :command "eval"
         keywords))

(define (handle-dsmp-body command body table in out . keywords)
  (let-keywords* keywords ((response-handler (lambda (x) x))
                           (get-handler (lambda (x) x))
                           (post-handler (lambda (x) x)))
    (cond ((string=? "eval" command)
           (with-error-handler
            (lambda (e) (make <dsmp-error>
                          :message (slot-ref e 'message)
                          :host 
                          :port
                          :stack-trace (cdr (vm-get-stack-trace))))
            (lambda ()
              (apply (unmarshal table (car body))
                     (map (lambda (elem)
                            (let ((obj (unmarshal table elem)))
                              (if (need-remote-eval? obj table)
                                  (lambda arg
                                    (eval-in-remote obj arg table
                                                    in out
                                                    :post-handler post-handler))
                                  obj)))
                          (cdr body))))))
          ((string=? "response" command) (response-handler body))
          ((string=? "get" command) (get-handler body))
          (else (error "unknown command" command)))))

(define (dsmp-response table input output . keywords)
  (let-keywords* keywords ((eof-handler (lambda ()
                                          (print "Got eof from client"))))
    (receive (header body)
        (read-dsmp input :eof-handler eof-handler)
      (let ((marshalized-body (marshal
                               table
                               (apply handle-dsmp-body
                                      (command-of header)
                                      body table
                                      input output
                                      keywords))))
        (dsmp-write (dsmp-header->string
                     (make-dsmp-header-from-string marshalized-body
                                                   :command "response"))
                    marshalized-body
                    output)))))

(provide "dsm/common")