(define-module dsm.error
  (extend dsm.dsm)
  (use gauche.vm.debugger)
  (use msm.marshal)
  (export make-dsm-error dsm-error?
          message-of stack-trace-of))
(select-module dsm.error)

(define-class <dsm-error> ()
  ((message :init-keyword :message :accessor message-of)
   (stack-trace :init-keyword :stack-trace :accessor stack-trace-of)))

(define-method initialize ((self <dsm-error>) args)
  (next-method)
  (let-keywords* args ((stack-trace-source #f))
    (if stack-trace-source
        (set! (stack-trace-of self)
              (make-stack-trace-from-source stack-trace-source)))))

(define (make-stack-trace-from-source src)
  (map (lambda (s)
         (if (car s)
             (pair-attribute-set! (cadr s) 'source-info (car s)))
         (cdr s))
       src))

(define (make-stack-trace-source st)
  (map (lambda (s)
         (let ((info (if (pair? (car s))
                         (pair-attribute-get (car s) 'source-info #f)
                         #f)))
           (cons (if info
                     (cons (format "~a:~a"
                                   (sys-gethostname)
                                   (car info))
                           (cdr info))
                     #f)
                 s)))
       st))

(define-method write-object ((self <dsm-error>) out)
  (format out "#,(<dsm-error> :message ~s :stack-trace-source ~a)"
          (message-of self)
          (marshal (make-marshal-table)
                   (make-stack-trace-source (stack-trace-of self)))))

(define-reader-ctor '<dsm-error>
  (lambda args
    (apply make <dsm-error> args)))

(define-method marshallable? ((self <dsm-error>))
  #t)

(define (dsm-error? obj)
  (is-a? obj <dsm-error>))

(define (make-dsm-error message)
  (make <dsm-error>
    :message message
    :stack-trace (cdr (vm-get-stack-trace))))

(provide "dsm/error")
