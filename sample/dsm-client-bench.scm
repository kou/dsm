#!/usr/bin/env gosh

(use gauche.time)
(use dsm.client)

(define (main arg)
  (let ((client (connect-server :host "localhost" :port 6789)))
    (time ((client "/plus-proc") 1 2))
    (time ((client "/plus-macro") 1 2))
    (time ((client "/for-each") identity '("Hello" "World")))
    (let ((x 10))
      (time ((client "/map") (lambda (elem) (+ elem x))
                              '(1 2))))
    (time (client "/long-string"))))
