(define-module dsm.common
  (extend dsm.error dsm.protocol)
  (use rfc.uri)
  (use srfi-13)
  (export parse-uri))
(select-module dsm.common)

(define (parse-uri uri)
  (receive (scheme specific)
      (uri-scheme&specific uri)
    (receive (authority path query fragment)
        (uri-decompose-hierarchical specific)
      (receive (user-info host port)
          (uri-decompose-authority authority)
        (values (and scheme (string->symbol scheme))
                user-info
                (if (string-null? host) #f host)
                (and port (string->number port))
                (if (string-null? path) #f path)
                query fragment)))))

(provide "dsm/common")
