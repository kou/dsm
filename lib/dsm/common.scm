(define-module dsm.common
  (extend dsm.dsm)
  (use text.tree)
  (use srfi-10)
  (use srfi-13)
  (use gauche.time)
  (use gauche.net)
  (use gauche.selector)
  (use gauche.charconv)
  (use gauche.vm.debugger)
  (use marshal)
  (export dsmp-request dsmp-response
          version-of encoding-of size-of)
  )
(select-module dsm.common)

(define dsmp-version 1)
(define dsmp-delimiter ";")

(define *dsmp-debug* #f)

(define (debug . messages)
  (if *dsmp-debug*
    (let ((printer (if (symbol-bound? 'p)
                     p
                     print)))
      (apply printer messages))))

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
                     ("e" . ,(or (ces-guess-from-string str "*JP")
                                 "UTF-8"))
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
  (debug (list "writing..." output))
  (debug (list "write" header body))
  (display header output)
  (display "\n" output)
  (display body output)
  (flush output))

(define (dsmp-read input . keywords)
  (let-keywords* keywords ((eof-handler (lambda () "Got eof")))
    (debug (list "reading..." input))
    (let* ((header (dsmp-read-header input eof-handler))
           (body (dsmp-read-body header input eof-handler)))
      (debug (list "read" (dsmp-header->string header) body))
      (values header body))))

(define (read-with-timeout input reader timeout not-response-handler)
  (if (char-ready? input)
    (reader input)
    (let ((result #f)
          (selector (make <selector>)))
      (selector-add! selector
                     input
                     (lambda (in . args)
                       (selector-delete! selector input #f #f)
                       (set! result (reader in)))
                     '(r))
         (if (zero? (selector-select selector timeout))
           (not-response-handler)
           result))))

(define (dsmp-read-header input eof-handler)
  (debug (list "reading header..."))
  (let ((header (read-with-timeout input read-line (list 3 0)
                                   (lambda () (error "not response")))))
    (debug header)
    (if (eof-object? header)
        (eof-handler)
        (parse-dsmp-header header))))

(define (dsmp-read-body header input eof-handler)
  (read-from-string (ces-convert
                     (read-required-block input (size-of header) eof-handler)
                     (encoding-of header))))

(define (read-required-block input size eof-handler)
  (define (more-read size)
    (read-with-timeout input (make-reader size) (list 3 0)
                       (lambda () (error "not response"))))
  
  (define (make-reader size)
    (lambda (in . args)
      (read-block size in)))

  (define (read-more-if-need block)
    (debug (list "read body" block))
    (cond ((eof-object? block) (eof-handler))
          ((< (string-size block) size)
           (debug (list "more reading..." size (string-size block)
                        (- size (string-size block))))
           (read-more-if-need
            (string-append block
                           (more-read (- size (string-size block))))))
          (else
           (debug (list "got block" block))
           block)))
    
  (debug (list "reading body..."))
  (read-more-if-need (more-read size)))

(define (need-remote-eval? obj table)
  (and (reference-object? obj)
       (not (using-same-table? table obj))))

(define (dsmp-request marshaled-obj table in out . keywords)
  (define (show-error message stack-trace)
    (display (format "~a\n" message)
             (current-error-port))
    (with-module gauche.vm.debugger
      (debug-print-stack stack-trace
                         *stack-show-depth*)))
  (define (raise-if-error obj)
    (if (dsmp-error? obj)
        (error (with-output-to-string
                 (lambda ()
                   (with-error-to-port
                    (current-output-port)
                    (lambda ()
                      (show-error (message-of obj)
                                  (stack-trace-of obj)))))))
        obj))
  
  (let-keywords* keywords ((command "get")
                           (get-handler raise-if-error)
                           (post-handler raise-if-error)
                           (eof-handler (lambda ()
                                          (print "Got eof from server"))))
    (define (dsmp-handler)
      (dsmp-write (dsmp-header->string
                   (make-dsmp-header-from-string marshaled-obj
                                                 :command command))
                  marshaled-obj
                  out)
      (receive (header body)
          (dsmp-read in :eof-handler eof-handler)
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

(define (eval-in-anonymous-module proc args)
  (define (%escape-value x)
    (cond ((list? x) (cons list (map %escape-value x)))
          ((symbol? x) `',x)
          (else x)))
  (let ((mod (make-module #f))
        (%proc (gensym)))
    (eval `(define ,%proc ,proc) mod)
    (eval (cons %proc (map %escape-value args))
          mod)))

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
              (eval-in-anonymous-module
               (unmarshal table (car body))
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
        (dsmp-read input :eof-handler eof-handler)
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