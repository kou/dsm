#!/usr/bin/env gosh

(use test.unit)
(use dsm.server)

(let ((server #f))
  (define-test-case "dsm server test"
    (setup
     (lambda () (set! server (make-dsm-server "dsmp://:11111"))))
    (teardown
     (lambda () (dsm-server-stop! server)))
    ("mount test"
     (assert-each assert-equal
                  `(("integer" . 1)
                    ("string" . "str")
                    ("symbol" . sym)
                    ("list" . ())
                    ("vector" . #())
                    ("procedure" . ,(lambda () #f))
                    )
                  :prepare (lambda (item)
                             (let ((key (car item))
                                   (value (cdr item)))
                               (add-mount-point! server key value)
                               (list value
                                     (get-by-mount-point server key)))))
     )
    ("get-by-id test"
     (let ((id-get (with-module marshal id-get))
           (marshal-table-of (with-module dsm.server marshal-table-of)))
       (assert-each assert-equal
                    `(1 "str" sym () #() ,(lambda () #f))
                    :prepare (lambda (obj)
                               (let ((id (id-get (marshal-table-of server)
                                                 obj)))
                                 (list obj
                                     (get-by-id server id)))))))))