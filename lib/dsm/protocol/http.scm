(define-module dsm.protocol.http
  (extend dsm.protocol.base)
  (use srfi-13)
  (use www.cgi)
  (use text.tr)
  (use util.list)
  (use gauche.parameter)
  (use dsm.utils)
  (use dsm.protocol.header)
  (export <dsmp-over-http>))
(select-module dsm.protocol.http)

;;; doh = dsmp over HTTP

;; (debug-on!)

(define doh-version 1)
(define doh-delimiter "\r\n")

(define get-meta
  (with-module www.cgi get-meta))

(define-class <dsmp-over-http> (<dsm-protocol>)
  ((version :accessor version-of :init-value doh-version)))

(define-method dsm-header->string ((self <dsmp-over-http>) header)
  (string-join (list #`"HTTP/1.1 GET ,(or (path-of self) \"/\")"
                     #`"X-DOH-Version: ,(version-of self)"
                     #`"Content-Type: text/x-s-expression; charest=,(encoding-of header)"
                     #`"Content-Length: ,(size-of header)"
                     #`"X-DOH-Command: ,(command-of header)")
               doh-delimiter))

(define-method dsm-read-header ((self <dsmp-over-http>) input eof-handler)
  (define counter 3)
  (define (next-line)
    (read-with-timeout input read-line (list 10 0)
                       (lambda (retry)
                         (dec! counter)
                         (if (> counter 0)
                           (retry)
                           (error "not response")))))
  
  (let loop ((result '())
             (line (next-line)))
    (cond ((eof-object? line) line)
          ((string-null? line) result)
          (else
           (loop (cons (let ((key&value (string-split line #/\s*:\s*/)))
                         (if (= 2 (length key&value))
                           (list (string-tr (string-upcase (car key&value))
                                            "\-" "_")
                                 (cadr key&value))
                           key&value))
                       result)
                 (next-line))))))

(define-method dsm-read-body ((self <dsmp-over-http>) header input eof-handler)
  (read-required-block input (size-of header) eof-handler))

(define-method dsm-write-header ((self <dsmp-over-http>) header output)
  (display header output)
  (display "\r\n\r\n" output))

(define-method dsm-write-body ((self <dsmp-over-http>) body output)
  (display body output))

(define-method parse-header ((self <dsmp-over-http>) alist)
  (make-dsm-header (make <dsmp-over-http>)
                   (parameterize ((cgi-metavariables alist))
                     `(("version" . ,(get-meta "X_DOH_VERSION"))
                       ("encoding" . ,(let ((type (get-meta "CONTENT_TYPE")))
                                        (if type
                                          (car (assoc-ref (map (lambda (field)
                                                                 (string-split field #/=\s*/))
                                                               (string-split type #/\;\s*/))
                                                          "charset"
                                                          '(#f)))
                                          (gauche-character-encoding))))
                       ("size" . ,(get-meta "CONTENT_LENGTH"))
                       ("command" . ,(get-meta "X_DOH_COMMAND"))))))

(define-method accept-version? ((self <dsmp-over-http>) version)
  (>= doh-version version))

(provide "dsm/protocol/dsmp")
