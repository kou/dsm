#!/usr/bin/env gosh

(use gauche.time)
(use benchmark)
(use dsm.client)

(define (main arg)
  (let ((client (dsm-connect-server "dsmp://localhost:6789")))
    (bm (lambda (r)
          (report r (lambda () (client "/one"))
                  :label "/one")
          (report r (lambda () (client "/plus-proc"))
                  :label "/plus-proc(no apply)")
          (report r (lambda () ((client "/plus-proc") 1 2))
                  :label "/plus-proc")
          (report r (lambda () ((client "/plus-macro") 1 2))
                  :label "/plus-macro")
          (report r (lambda ()
                      ((client "/for-each") identity '("Hello" "World")))
                  :label "/for-each")
          (let ((x 10))
            (report r (lambda ()
                        ((client "/map") (lambda (elem) (+ elem x))
                         '(1 2)))
                    :label "/map"))
          (report r (lambda () (client "/long-string"))
                  :label "/long-string"))
        :label-width 20)))

