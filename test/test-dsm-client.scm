#!/usr/bin/env gosh

(use test.unit)
(use gauche.process)
(use dsm.client)
(use rfc.uri)
(load "test/dsm-server-conf")

(define (connect-dsmp-server host port)
  (dsm-connect-server (uri-compose :scheme "dsmp"
                                   :host host
                                   :port port)))

(define (test-dsm-server-run command host port)
  (let ((server (run-process command
                             #`"--host=,host"
                             #`"--port=,port")))
    (do ()
        ((with-error-handler
             (lambda (e) #f)
           (lambda ()
             ((connect-dsmp-server host port))
             #t)))
      (sys-nanosleep 1000)) ; wait for starting server
    server))

(let* ((server-command "./test/dsm-server.scm")
       (server-host "localhost")
       (server-port 59104)
       (process '(test-dsm-server-run server-command server-host server-port)))
  (define-test-case "dsm client/server connection test"
     (setup
      (lambda ()
        (set! process
              (test-dsm-server-run server-command server-host server-port))))
     (teardown
      (lambda ()
        (process-kill process)))
    ("marshalizable object test"
     (let ((server (connect-dsmp-server server-host server-port)))
       (for-each (lambda (key&value)
                   (assert-equal (cdr key&value) (server (car key&value))))
                 marshalizable-key&value-alist)))
    ("marshal procedure test"
     (let ((server (connect-dsmp-server server-host server-port)))
       (for-each (lambda (elem)
                   (assert-equal (caddr elem)
                                 (apply (server (car elem)) (cdddr elem))))
                 procedure-list)))
    ("multiple client test"
     (for-each (lambda (elem)
                 (let ((server (connect-dsmp-server server-host server-port)))
                   (assert-equal (caddr elem)
                                 (apply (server (car elem)) (cdddr elem)))))
               procedure-list))))
