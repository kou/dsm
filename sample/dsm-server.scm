#!/usr/bin/env gosh

(use dsm.server)

(define (main arg)
  (let ((server (make-dsm-server :port 6789)))
    (add-mount-point! server "/plus" (lambda (x y) (+ x y)))
    (add-mount-point! server '/map map)
    (start-dsm-server server)))
