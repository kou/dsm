#!/usr/bin/env gosh

(use dsm.client)

(define (main arg)
  (let ((client (dsm-connect-server "dsmp://localhost:6789")))
    (print ((client "/plus-proc") 1 2))
    (print ((client "/plus-macro") 1 2))
    ((client "/for-each") display '("Hello" "World"))
    (newline)
    (let ((x 10))
      (print ((client "/map") (lambda (elem) (+ elem x))
                              '(1 2))))
    (print (client "/long-string"))
    ))
