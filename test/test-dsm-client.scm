#!/usr/bin/env gosh

(use test.unit)
(use gauche.process)
(use dsm.client)
(load "test/dsm-server-conf")

(let* ((server-command "./test/dsm-server.scm")
       (server-host "localhost")
       (server-port 59102)
       (process #f))
  (define-test-case "Server test"
     (setup
      (lambda ()
        (set! process
              (begin0
                  (run-process server-command
                               "--host" server-host
                               "--port" server-port)
                (sys-nanosleep 500000000) ; wait for starting server
                ))))
     (teardown
      (lambda ()
        (process-kill process)))
    ("marshalizable object test"
     (let ((server (connect-server :host server-host
                                   :port server-port)))
       (for-each (lambda (key&value)
                   (assert-equal (cdr key&value) (server (car key&value))))
                 marshalizable-key&value-alist)))
    ("marshal procedure test"
     (let ((server (connect-server :host server-host
                                   :port server-port)))
       (for-each (lambda (elem)
                   (assert-equal (caddr elem)
                                 (apply (server (car elem)) (cdddr elem))))
                 procedure-list)))
    ("multiple client test"
     (for-each (lambda (elem)
                 (let ((server (connect-server :host server-host
                                               :port server-port)))
                   (assert-equal (caddr elem)
                                 (apply (server (car elem)) (cdddr elem)))))
               procedure-list))
    ))
