(define-module dsm.common
  (extend dsm.error dsm.protocol)
  (use rfc.uri)
  (use srfi-13)
  (export parse-uri))
(select-module dsm.common)

(define (parse-uri uri)
  (define (filter-non-empty-string str)
    (and (string? str)
         (not (string-null? str))
         str))

  (define (convert-if-not-false obj converter)
    (and obj (converter obj)))
  
  (receive (scheme specific)
      (uri-scheme&specific uri)
    (receive (authority path query fragment)
        (uri-decompose-hierarchical specific)
      (receive (user-info host port)
          (uri-decompose-authority authority)
        (values (convert-if-not-false scheme string->symbol)
                user-info
                (filter-non-empty-string host)
                (convert-if-not-false port string->number)
                (filter-non-empty-string path)
                query
                fragment)))))

(provide "dsm/common")
