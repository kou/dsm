(define-module dsm.protocol.dsmp
  (extend dsm.protocol.base)
  (use dsm.protocol.header)
  (use srfi-13)
  (export <dsmp>))
(select-module dsm.protocol.dsmp)

(define dsmp-version 1)
(define dsmp-delimiter ";")

(define-class <dsmp> (<dsm-protocol>)
  ((version :accessor version-of :init-value dsmp-version)))

(define-method dsm-header->string ((self <dsmp>) header)
  (string-join (list #`"v=,(version-of self)"
                     #`"e=,(encoding-of header)"
                     #`"s=,(size-of header)"
                     #`"c=,(command-of header)")
               dsmp-delimiter))

(define-method dsm-read-header
    ((self <dsmp>) input eof-handler not-response-handler)
  (define counter 3)
  (read-with-timeout input read-line (list 10 0)
                     (lambda (retry)
                       (dec! counter)
                       (if (> counter 0)
                         (retry)
                         (not-response-handler)))))

(define-method dsm-read-body
    ((self <dsmp>) header input eof-handler not-response-handler)
  (read-required-block input (size-of header)
                       eof-handler not-response-handler))

(define-method dsm-write-header ((self <dsmp>) header output)
  (display header output)
  (display "\n" output))

(define-method dsm-write-body ((self <dsmp>) body output)
  (display body output))

(define-method parse-header ((self <dsmp>) str)
  (make-dsm-header (make <dsmp>)
                   (map (lambda (elem)
                          (let ((splited-elem (string-split elem "=")))
                            (cons (car splited-elem)
                                  (cadr splited-elem))))
                        (string-split (string-trim-right str)
                                      dsmp-delimiter))))

(define-method accept-version? ((self <dsmp>) version)
  (>= dsmp-version version))

(provide "dsm/protocol/dsmp")
