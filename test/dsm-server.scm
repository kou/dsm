#!/usr/bin/env gosh

(use srfi-13)
(use srfi-37)
(require "test/dsm-server-conf")
(use dsm.server)

(define (to-daemon)
  (close-input-port (standard-input-port))
  (close-output-port (standard-output-port))
  (close-output-port (standard-error-port)))

(define (main args)
  (define default-host "localhost")
  (define default-port 59102)
  (define (usage)
    (print
     #`"\t-h, --host=HOST\t\tUse the given HOST. (default ,|default-host|)")
    (print
     #`"\t-p, --port=PORT\t\tUse the given PORT. (default ,|default-port|)")
    (print "\t-h, --help\t\tDisplay this help.")
    (exit 0))
  (define options
    (list (option '(#\h "host") #f #t
                  (lambda (option name arg host port . others)
                    (values (if (string? arg) arg host)
                            port)))
          (option '(#\p "port") #f #t
                  (lambda (option name arg host port . others)
                    (values host
                            (if (string? arg) arg port))))
          (option '(#\h "help") #f #f
                  (lambda (option name arg . others)
                    (usage)))))
   (receive (host port)
     (args-fold (cdr args)
       options
       (lambda (option name arg . seeds) ; unrecognized
         (print "Unrecognized option:" name)
         (usage))
       (lambda (operand host port)      ; operand
         (values host port))
       default-host
       default-port
       )
     (let ((server (make-dsm-server #`"dsmp://,|host|:,|port|")))
       (for-each (lambda (elem)
                   (apply add-mount-point! server elem))
                 marshalizable-key&value-list)
       (for-each (lambda (elem)
                   (add-mount-point! server (car elem) (cadr elem)))
                 procedure-list)
       (dsm-server-start! server)
       (dsm-server-join! server)
       (dsm-server-stop! server)))
   0)
