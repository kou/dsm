(define-module dsm.server
  (extend dsm.dsm)
  (use srfi-13)
  (use gauche.net)
  (use gauche.threads)
  (use gauche.selector)
  (use gauche.parameter)
  (use marshal)
  (use dsm.common)
  (export make-dsm-server start-dsm-server stop-dsm-server
          socket-of
          add-mount-point! get-by-mount-point get-by-id
          require-in-root-thread required-in-root-thread?)
  )
(select-module dsm.server)

(define-class <dsm-server> ()
  ((host :init-keyword :host :accessor host-of :init-value #f)
   (port :init-keyword :port :accessor port-of :init-value 59102)
   (socket :accessor socket-of)
   (mount-table :accessor mount-table-of
                :init-form (make-hash-table 'string=?))
   (marshal-table :accessor marshal-table-of
                  :init-form (make-marshal-table))
   (timeout :init-keyword :timeout :accessor timeout-of
            :init-value '(1 0))
   ))

(define-method initialize ((self <dsm-server>) args)
  (next-method)
  (set! (socket-of self)
        (make-server-socket 'inet
                            (port-of self)
                            :reuse-addr? #t)))

(define (make-dsm-server . keywords)
  (apply make <dsm-server> keywords))

(define-method add-mount-point! ((self <dsm-server>) mount-point value)
  (hash-table-put! (mount-table-of self)
                   (x->string mount-point)
                   value))

(define-method get-by-mount-point ((self <dsm-server>) mount-point)
  (hash-table-get (mount-table-of self)
                  (x->string mount-point)))

(define-method get-by-id ((self <dsm-server>) id)
  (id-ref (marshal-table-of self) id))

(define-class <thread-pool> ()
  ((pool :accessor pool-of :init-form '())
   (max :accessor max-of :init-value #f)))

(define-method thread-pool-push! ((self <thread-pool>) thunk)
  (if (eq? 'none (gauche-thread-type))
    (thunk)
    (thread-start! (make-thread thunk))))

(define dsm-cv (make-parameter (make-condition-variable)))
(define dsm-mu (make-parameter (make-mutex)))
(define (require-in-root-thread path mod)
  (mutex-lock! (dsm-mu))
  (condition-variable-specific-set! (dsm-cv) (cons path mod))
  (mutex-unlock! (dsm-mu))
  (condition-variable-signal! (dsm-cv))
  (thread-yield!))

(define (required-in-root-thread?)
  (eq? #t (condition-variable-specific (dsm-cv))))

(define (reset-required-in-root-thread)
  (condition-variable-specific-set! (dsm-cv) #f))

(define-method start-dsm-server ((self <dsm-server>))
  (let ((selector (make <selector>))
        (pool (make <thread-pool>)))

    (define (accept-handler sock flag)
      (let* ((client (socket-accept (socket-of self)))
             (output (socket-output-port client)))
        (selector-add! selector
                       (socket-input-port client :buffering? :none)
                       (lambda (input flag)
                         (thread-pool-push!
                          pool
                          (lambda ()
                            (handle-dsmp client input output))))
                       '(r))))
     
    (define (handle-dsmp client input output)
      (call/cc
       (lambda (cont)
         (dsmp-response (marshal-table-of self)
                        input output
                        :get-handler (cut get-by-mount-point self <>)
                        :eof-handler (cut (make-eof-handler cont)
                                          client input output)))))
    
    (define (make-eof-handler return)
      (lambda (client input output)
        (selector-delete! selector input #f #f)
        (socket-shutdown client 2)
        (return 0)))
                     
    (selector-add! selector
                   (socket-fd (socket-of self))
                   accept-handler
                   '(r))
    (parameterize ((dsm-cv (make-condition-variable))
                   (dsm-mu (make-mutex)))
      (reset-required-in-root-thread)
      (do () ((eq? 'shutdown (socket-status (socket-of self))))
        (mutex-lock! (dsm-mu))
        (if (pair? (condition-variable-specific (dsm-cv)))
          (begin
            (p (current-thread) (condition-variable-specific (dsm-cv)))
            ;; (load (condition-variable-specific (dsm-cv)))
            (eval `(require ,(car (condition-variable-specific (dsm-cv))))
                  (cdr (condition-variable-specific (dsm-cv))))
            (condition-variable-specific-set! (dsm-cv) #t)
            (mutex-unlock! (dsm-mu))
            (thread-yield!))
          (mutex-unlock! (dsm-mu)))
        (selector-select selector (timeout-of self))))))

(define-method stop-dsm-server ((self <dsm-server>))
  (socket-close (socket-of self))
  (socket-shutdown (socket-of self) 2))

(provide "dsm/server")
