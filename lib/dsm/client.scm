(define-module dsm.client
  (extend dsm.dsm)
  (use srfi-13)
  (use gauche.net)
  (use marshal)
  (use dsm.common)
  (export connect-server)
  )
(select-module dsm.client)

(define-class <dsm-client> ()
  ((host :init-keyword :host :accessor host-of :init-value #f)
   (port :init-keyword :port :accessor port-of :init-value 59102)
   (socket :accessor socket-of)
   ))

(define-method initialize ((self <dsm-client>) args)
  (next-method)
  (slot-set! self 'socket
             (make-client-socket 'inet
                                 (host-of self)
                                 (port-of self))))

(define (connect-server . keywords)
  (let* ((client (apply make <dsm-client> keywords))
         (socket (socket-of client))
         (in (socket-input-port socket))
         (out (socket-output-port socket))
         (table (make-marshal-table)))
    (lambda (mount-point)
      (apply dsmp-request
             (marshal table mount-point) table
             in out
;;             :post-handler (lambda (obj) obj)
             keywords))))

(provide "dsm/client")