#!/usr/bin/env gosh

(use test.unit)

(define-module test-dsm-common
  (use srfi-13)
  (use rfc.uri)
  (use test.unit)
  (use msm.marshal)
  (use dsm.common))

(select-module test-dsm-common)
(define (x->dsm-header->string table obj . others)
  (with-module dsm.protocol
    (apply x->dsm-header->string table obj others)))

(define-assertion (assert-dsm-header prot header version encoding size command)
  (define (make-message-handler expect type)
    (lambda (actual)
      (format " expected <~s> ~a\n  but was <~s>"
              expect type actual)))
  (let* ((parsed-header (with-module dsm.protocol (parse-header prot header)))
         (header-version (with-module dsm.protocol (version-of parsed-header)))
         (header-encoding (with-module dsm.protocol (encoding-of parsed-header)))
         (header-size (with-module dsm.protocol (size-of parsed-header)))
         (header-command (with-module dsm.protocol (command-of parsed-header))))
    (assert-equal header-version version
                  (make-message-handler header-version "version"))
    (assert-equal header-encoding encoding
                  (make-message-handler header-encoding "encoding"))
    (assert-equal header-size size
                  (make-message-handler header-size "size"))
    (assert-equal header-command command
                  (make-message-handler header-command "command"))
    ))

(let ((table #f)
      (dsmp (make <dsmp>)))
  (define-test-case "dsm common library test"
    (setup
     (lambda () (set! table (make-marshal-table))))
    ("make-header test"
     (let ((encoding (string-upcase
                      (symbol->string (gauche-character-encoding)))))
       (assert-each assert-equal
                    `((,#`"v=1;e=,|encoding|;s=1;c=get" . 1)
                      (,#`"v=1;e=,|encoding|;s=5;c=get" . "abc")
                      (,#`"v=1;e=,|encoding|;s=2;c=get" . ())
                      (,#`"v=1;e=,|encoding|;s=5;c=get" . (1 2)))
                    :prepare (lambda (item)
                               (list (car item)
                                     (x->dsm-header->string dsmp
                                                            table
                                                            (cdr item)
                                                            :command "get"))))))
    ("parse-header test"
     (assert-each assert-dsm-header
                  `((,dsmp "v=1;e=UTF-8;s=1;c=get\n" 1 "UTF-8" 1 "get")
                    (,dsmp "version=1;encoding=UTF-8;size=1;command=eval\n"
                           1 "UTF-8" 1 "eval")
                    (,dsmp "v=0.9;e=EUC-JP;s=3;c=get\n" 0.9 "EUC-JP" 3 "get"))))
    ("dsm-response test"
     (assert-each assert-equal
                  `(1 "abc" ,(lambda (x) x))
                  :prepare
                  (lambda (item)
                    (let ((dsm-str (string-append
                                    (x->dsm-header->string dsmp
                                                           table item
                                                           :command
                                                           "response")
                                    "\n"
                                    (marshal table item))))
                      (list dsm-str
                            (let ((in (open-input-string dsm-str))
                                  (out (open-output-string)))
                              (dsm-response dsmp
                                            table
                                            in out
                                            :response-handler
                                            (lambda (body)
                                              (unmarshal table body)))
                              (get-output-string out))))))
     )
    ("parse-uri test"
     (assert-each (lambda (excepted uri)
                    (receive all (parse-uri uri)
                      (assert-equal excepted all)))
                  `(((dsmp #f "localhost" 6789 #f #f #f)
                     "dsmp://localhost:6789")
                    ((dsmp #f #f 6789 #f #f #f)
                     "dsmp://:6789")
                    ((#f #f #f 6789 #f #f #f)
                     "//:6789")
                    ((http #f "example.com" 12345 "/" #f #f)
                     ,(uri-compose :scheme "http"
                                   :host "example.com"
                                   :port 12345)))))))
