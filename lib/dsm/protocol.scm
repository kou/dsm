(define-module dsm.protocol
  (extend dsm.protocol.dsmp
          dsm.protocol.http)
  (use gauche.charconv)
  (use marshal)
  (use dsm.utils)
  (use dsm.error)
  (use dsm.protocol.header)
  (export dsm-request dsm-response))
(select-module dsm.protocol)

(define (dsm-request protocol marshaled-obj table in out . keywords)
  (define (show-error message stack-trace)
    (display (format "~a\n" message)
             (current-error-port))
    (with-module gauche.vm.debugger
      (debug-print-stack stack-trace
                         *stack-show-depth*)))
  (define (raise-if-error obj)
    (if (dsm-error? obj)
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
                                          (print "Got eof from server")))
                           (not-response-handler
                            (lambda ()
                              (error "not response from server")))
                           (timeout (list 15 0)))
    (define (dsm-handler)
      (dsm-write protocol command marshaled-obj out)
      (receive (header body)
          (dsm-read protocol in
                    :eof-handler eof-handler
                    :not-response-handler not-response-handler
                    :timeout timeout)
        (handle-response header body)))

    (define (handle-response header body)
      (let ((obj (handle-dsm-body protocol
                                  (command-of header)
                                  body table
                                  in out
                                  :get-handler get-handler
                                  :response-handler response-handler
                                  :post-handler post-handler)))
        (cond ((string=? "eval" (command-of header))
               (let ((marshaled-obj (marshal table obj)))
                 (dsm-request protocol marshaled-obj table
                              in out
                              :command "response"
                              :get-handler get-handler
                              :post-handler post-handler
                              :eof-handler eof-handler
                              :not-response-handler not-response-handler)))
              (else obj))))
    
    (define (response-handler obj)
      (if (need-remote-eval? obj table)
          (lambda arg
            (eval-in-remote protocol obj arg table in out
                            :get-handler get-handler
                            :post-handler post-handler
                            :eof-handler eof-handler))
          (post-handler obj)))

    (dsm-handler)))

(define (dsm-response protocol table input output . keywords)
  (let-keywords* keywords ((eof-handler (lambda ()
                                          (print "Got eof from client")))
                           (not-response-handler
                            (lambda ()
                              (error "not response from client")))
                           (timeout (list 15 0)))
    (receive (header body)
        (dsm-read protocol input
                  :eof-handler eof-handler
                  :not-response-handler not-response-handler
                  :timeout timeout)
      (let ((marshalized-body (marshal
                               table
                               (apply handle-dsm-body
                                      protocol
                                      (command-of header)
                                      body table
                                      input output
                                      keywords))))
        (dsm-write protocol "response" marshalized-body output)))))

(define (x->dsm-header protocol table obj . keywords)
  (apply make-dsm-header-from-string protocol (marshal table obj) keywords))

(define (x->dsm-header->string protocol table obj . keywords)
  (dsm-header->string protocol
                      (apply x->dsm-header protocol table obj keywords)))

(define (dsm-write protocol command body output)
  (let ((header (dsm-header->string
                 protocol
                 (make-dsm-header-from-string protocol body
                                              :command command))))
    (debug (list "writing..." output))
    (dsm-write-header protocol header output)
    (dsm-write-body protocol body output)
    (flush output)
    (debug (list "write" header body))))

(define (dsm-read protocol input . keywords)
  (let-keywords* keywords ((eof-handler (lambda () "Got eof"))
                           (not-response-handler
                            (lambda () (error "not response")))
                           (timeout (list 15 0)))
    (define (read-header)
      (debug (list "reading header..."))
      (let ((header (dsm-read-header protocol input
                                     eof-handler not-response-handler
                                     timeout)))
        (debug header)
        (if (eof-object? header)
          (eof-handler)
          (parse-header protocol header))))

    (define (read-body header)
      (debug (list "reading body..."))
      (read-from-string (ces-convert
                         (dsm-read-body protocol header input
                                        eof-handler not-response-handler
                                        timeout)
                         (encoding-of header))))

    (debug (list "reading..." input))
    (let* ((header (read-header))
           (body (read-body header)))
      (debug (list "read" (dsm-header->string protocol header) body))
      (values header body))))

(define (need-remote-eval? obj table)
  (and (reference-object? obj)
       (not (using-same-table? table obj))))

(define (eval-in-remote protocol obj arg table in out . keywords)
  (apply dsm-request
         protocol
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

(define (handle-dsm-body protocol command body table in out . keywords)
  (let-keywords* keywords ((response-handler (lambda (x) x))
                           (get-handler (lambda (x) x))
                           (post-handler (lambda (x) x))
                           (eof-handler #f))
    (cond ((string=? "eval" command)
           (with-error-handler
            (lambda (e) (make-dsm-error (slot-ref e 'message)))
            (lambda ()
              (eval-in-anonymous-module
               (unmarshal table (car body))
               (map (lambda (elem)
                      (let ((obj (unmarshal table elem)))
                        (if (need-remote-eval? obj table)
                            (lambda arg
                              (apply eval-in-remote
                                     protocol obj arg table
                                     in out
                                     :post-handler post-handler
                                     (if eof-handler
                                       (list :eof-handler eof-handler)
                                       '())))
                            obj)))
                    (cdr body))))))
          ((string=? "response" command) (response-handler body))
          ((string=? "get" command) (get-handler body))
          (else (error "unknown command" command)))))

(provide "dsm/protocol")
