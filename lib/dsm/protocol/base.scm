(define-module dsm.protocol.base
  (extend dsm.dsm)
  (use gauche.selector)
  (use dsm.utils))
(select-module dsm.protocol.base)

(define-class <dsm-protocol> ()
  ((version :accessor version-of)
   (path :accessor path-of :init-keyword :path :init-value #f)))

(define-method dsm-header->string ((self <dsm-protocol>) header)
  (error "not implemented!"))
(define-method dsm-read-header
    ((self <dsm-protocol>) input eof-handler not-response-handler timeout)
  (error "not implemented!"))
(define-method dsm-read-body
    ((self <dsm-protocol>) header input eof-handler not-response-handler timeout)
  (error "not implemented!"))
(define-method dsm-write-header ((self <dsm-protocol>) header output)
  (error "not implemented!"))
(define-method dsm-write-body ((self <dsm-protocol>) body output)
  (error "not implemented!"))
(define-method parse-header ((self <dsm-protocol>) str)
  (error "not implemented!"))
(define-method accept-version? ((self <dsm-protocol>) version)
  (error "not implemented!"))


(define (read-with-timeout input reader timeout timeout-handler)
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
      (let ((retry #f))
        (if (zero? (begin
                     (call/cc
                      (lambda (cont)
                        (set! retry
                              (lambda () (cont 'retry)))))
                     (selector-select selector timeout)))
          (timeout-handler retry)
          result)))))

(define (read-required-block input size eof-handler not-response-handler timeout)
  (define (more-read size)
    (define retry-count 3)
    (read-with-timeout input (make-reader size) timeout
                       (lambda (retry)
                         (dec! retry-count)
                         (if (< retry-count 0)
                           (not-response-handler)
                           (retry)))))
  
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
    
  (read-more-if-need (more-read size)))

(provide "dsm/protocol/base")
