(define-module dsm.protocol.header
  (extend dsm.protocol.base)
  (use gauche.charconv)
  (use msm.marshal)
  (export version-of encoding-of size-of command-of
          make-dsm-header make-dsm-header-from-string))
(select-module dsm.protocol.header)

(define-class <dsm-header> ()
  ((protocol :init-keyword :protocol :accessor protocol-of)
   (encoding :init-keyword :encoding :accessor encoding-of)
   (size :init-keyword :size :accessor size-of)
   (command :init-keyword :command :accessor command-of)))

(define-method version-of ((self <dsm-header>))
  (version-of (protocol-of self)))

(define (make-dsm-header protocol alist)
  (define (set-dsm-header-slot! header key value)
    (cond ((rxmatch #/^v/ key)
           (let ((version (string->number value)))
             (if (accept-version? protocol version)
               (set! (version-of (protocol-of header)) version)
               (error (format #f "version mismatch. require: ~s; request: ~s."
                              (version-of protocol)
                              version)))))
          ((rxmatch #/^e/ key)
           (slot-set! header 'encoding value))
          ((rxmatch #/^c/ key)
           (slot-set! header 'command value))
          ((rxmatch #/^s/ key)
           (slot-set! header 'size (x->number value)))))

  (let ((dsm-header (make <dsm-header> :protocol protocol)))
    (for-each (lambda (elem)
                (set-dsm-header-slot! dsm-header
                                      (x->string (car elem)) (cdr elem)))
              alist)
    dsm-header))

(define (make-dsm-header-from-string protocol str . keywords)
  (make-dsm-header protocol
                   `(("e" . ,(or (ces-guess-from-string str "*JP")
                                 (gauche-character-encoding)
                                 ;;"UTF-8"
                                 ))
                     ("s" . ,(string-size str))
                     ("c" . ,(get-keyword :command keywords)))))

(provide "dsm/protocol/header")
