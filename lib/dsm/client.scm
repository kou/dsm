(define-module dsm.client
  (extend dsm.dsm)
  (use srfi-13)
  (use gauche.net)
  (use marshal)
  (use dsm.common)
  (export dsm-connect-server path-of)
  )
(select-module dsm.client)

(define-class <dsm-client> ()
  ((uri :init-keyword :uri :accessor uri-of)
   (socket :accessor socket-of)
   (protocol :accessor protocol-of)
   ))

(define-method initialize ((self <dsm-client>) args)
  (define (invalid-type-message type)
    #`",|type| not specified in uri ,(uri-of self)")
  
  (next-method)
  (receive (scheme user-info host port path query fragment)
      (parse-uri (uri-of self))
    (set! (socket-of self)
          (apply make-client-socket
                 (case scheme
                   ((dsmp http)
                    (let ((invalid-type (cond ((not host) 'host)
                                              ((not port) 'port)
                                              (else #f))))
                      (if invalid-type
                        (error (invalid-type-message invalid-type))))
                    (list 'inet host port))
                   ((unix)
                    (unless path
                      (error (invalid-type-message 'path)))
                    (list 'unix path))
                   (else (error "unknown scheme: " scheme)))))
    (set! (protocol-of self)
          (case scheme
            ((dsmp unix) (make <dsmp> :path path))
            ((http) (make <dsmp-over-http> :path path))
            (else (error "unknown scheme: " scheme))))))

(define-method path-of ((self <dsm-client>))
  (path-of (protocol-of self)))

(define (dsm-connect-server uri . keywords)
  (let* ((client (apply make <dsm-client> :uri uri keywords))
         (socket (socket-of client))
         (in (socket-input-port socket :buffering :modest))
         (out (socket-output-port socket :buffering :full))
         (table (make-marshal-table)))
    (lambda args
      (let-optionals* args ((mount-point #f))
        (if mount-point
          (if (eq? 'CONNECTED (socket-status (socket-of client)))
            (apply dsm-request
                   (protocol-of client)
                   (marshal table mount-point)
                   table in out
                   ;; :post-handler (lambda (obj) obj)
                   keywords)
            (error "doesn't connected."))
          (socket-shutdown (socket-of client) 2))))))

(provide "dsm/client")
