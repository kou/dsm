(define-module dsm.server
  (use srfi-13)
  (use gauche.net)
  (use gauche.selector)
  (use marshal)
  (use dsm.common)
  (export make-dsm-server start-dsm-server stop-dsm-server
          socket-of
          add-mount-point! get-by-mount-point get-by-id)
  )
(select-module dsm.server)

(define-class <dsm-server> ()
  ((host :init-keyword :host :accessor host-of :init-value #f)
   (port :init-keyword :port :accessor port-of :init-value 59102)
   (mount-table :accessor mount-table-of)
   (socket :accessor socket-of)
   (marshal-table :accessor marshal-table-of)
   (timeout :init-keyword :timeout :accessor timeout-of
            :init-value '(1 0))
   ))

(define-method initialize ((self <dsm-server>) args)
  (next-method)
  (slot-set! self 'mount-table (make-hash-table 'string=?))
  (slot-set! self 'socket (make-server-socket 'inet
                                              (port-of self)
                                              :reuse-addr? #t))
  (slot-set! self 'marshal-table (make-marshal-table))
  )

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

(define-method start-dsm-server ((self <dsm-server>))
  (let ((selector (make <selector>))
        (exit-current-client #f))

    (define (accept-handler sock flag)
      (let* ((client (socket-accept (socket-of self)))
             (output (socket-output-port client)))
        (selector-add! selector
                       (socket-input-port client :buffered? #f)
                       (lambda (input flag)
                         (handle-dsmp client input output))
                       '(r))))
    
    (define (handle-dsmp client input output)
      (call/cc
       (lambda (cont)
         (set! exit-current-client cont)
         (dsmp-response (marshal-table-of self)
                        input output
                        :get-handler (cut get-by-mount-point self <>)
                        :eof-handler (cut eof-handler client input output)))))

    (define (eof-handler client input output)
      (selector-delete! selector input #f #f)
      (socket-shutdown client 2)
      (exit-current-client 0))
                     
    (selector-add! selector
                   (socket-fd (socket-of self))
                   accept-handler
                   '(r))
    (do () ((eq? 'shutdown (socket-status (socket-of self))))
      (selector-select selector (timeout-of self)))))

(define-method stop-dsm-server ((self <dsm-server>))
  (socket-close (socket-of self))
  (socket-shutdown (socket-of self) 2))

(provide "dsm/server")