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

(define (test-dsm-server-stop server)
  (process-kill server)
  (do ()
      ((not (process-alive? server)))
    (process-wait server #t)))

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
       (test-dsm-server-stop process)))
    ("marshal procedure test"
     (let ((server (connect-dsmp-server server-host server-port)))
       (assert-each (lambda (key proc expect . args)
                      (assert-equal expect
                                    (apply (server key) args)))
                    procedure-list
                    :apply-if-can #t)))
    ("error procedure test"
     (let ((server (connect-dsmp-server server-host server-port)))
       (assert-each (lambda (key proc expected . args)
                      (assert-error-message expected
                                            (lambda ()
                                              (apply (server key) args))))
                    error-procedure-list
                    :apply-if-can #t)))
    ("marshal procedure in collection test"
     (let ((server (connect-dsmp-server server-host server-port)))
       (assert-each (lambda (key proc expect . args)
                      (assert-equal expect
                                    (map (lambda (server-proc arg)
                                           (apply server-proc arg))
                                         (server key)
                                         args)))
                    procedure-in-collection-list
                    :apply-if-can #t)
       (assert-each (lambda (key proc expect . args)
                      (assert-equal expect
                                    (map (lambda (server-col arg)
                                           (apply (cadr server-col) arg))
                                         (server key)
                                         args)))
                    procedure-in-collection2-list
                    :apply-if-can #t)))
    ("marshallable object test"
     (let ((server (connect-dsmp-server server-host server-port)))
       (assert-each (lambda (key value)
                      (assert-equal value (server key)))
                    marshallable-key&value-list
                    :apply-if-can #t)))
    ("multiple client test"
     (assert-each (lambda (key proc expect . args)
                    (let ((server (connect-dsmp-server server-host
                                                       server-port)))
                      (assert-equal expect
                                    (apply (server key) args))))
                  procedure-list
                  :apply-if-can #t))))
