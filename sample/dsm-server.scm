#!/usr/bin/env gosh

(use dsm.server)

(define-macro (plus-macro x y)
  `(+ ,x ,y))

(define (main arg)
  (let ((server (make-dsm-server :port 6789)))
    (add-mount-point! server "/one" 1)
    (add-mount-point! server "/plus-proc" (lambda (x y) (+ x y)))
    (add-mount-point! server "/plus-macro" plus-macro)
    (add-mount-point! server '/for-each for-each)
    (add-mount-point! server '/map map)
    (add-mount-point! server '/long-string
                      (call-with-input-file "lib/dsm/common.scm" port->string))
    (start-dsm-server server)
    (dsm-server-join! server)
    (stop-dsm-server server)))
