(define-module dsm.utils
  (extend dsm.dsm)
  (export debug debug-on! debug-off!))
(select-module dsm.utils)

(define *dsm-debug* #f)

(define (debug-on!)
  (set! *dsm-debug* #t))

(define (debug-off!)
  (set! *dsm-debug* #f))

(define-syntax debug
  (syntax-rules ()
    ((_ message ...)
     (if *dsm-debug*
       (let ((printer (if (symbol-bound? 'p)
                          p
                          print)))
         (printer message ...))
       #f))))

(provide "dsm/utils")