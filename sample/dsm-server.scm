#!/usr/bin/env gosh

(use dsm.server)

(define-macro (plus-macro x y)
  `(+ ,x ,y))

(define (main arg)
  (let ((server (make-dsm-server :port 6789)))
    (add-mount-point! server "/plus-proc" (lambda (x y) (+ x y)))
    (add-mount-point! server "/plus-macro" plus-macr)
    (add-mount-point! server '/map map)
    (start-dsm-server server)))
