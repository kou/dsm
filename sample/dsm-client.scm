#!/usr/bin/env gosh

(use dsm.client)

(define (main arg)
  (let ((client (connect-server :host "localhost" :port 6789)))
    (print ((client "/plus-proc") 1 2))
    (print ((client "/plus-macro") 1 2))
    (let ((x 10))
      (print ((client "/map") (lambda (elem) (+ elem x))
                              '(1 2))))))
