(define-module dsm.error
  (extend dsm.dsm)
  (use gauche.vm.debugger)
  (use msm.marshal)
  (export make-dsm-error dsm-error?
          message-of stack-trace-of show-stack-trace))
(select-module dsm.error)

(define *stack-depth-limit* 15)

(if (symbol-bound? 'pair-attribute-get)
  (begin
    (define (show-stack-trace stack-trace)
      (with-module gauche.vm.debugger
        (debug-print-stack stack-trace *stack-depth-limit*))))
  (begin
    (define pair-attribute-get
      (with-module gauche.internal pair-attribute-get))
    (define pair-attribute-set!
      (with-module gauche.internal pair-attribute-set!))
    (define extended-cons
      (with-module gauche.internal extended-cons))
    (define extended-list
      (with-module gauche.internal extended-list))
    (define (error-line stack-trace)
      (and-let* (((pair? stack-trace))
                 (info (pair-attribute-get stack-trace 'source-info #f))
                 ((pair? info))
                 ((pair? (cdr info))))
        (format #f "~a:~a: ~s" (car info) (cadr info) stack-trace)))
    (define (show-stack-trace stack-trace . options)
      (let-keywords* options ((lines '())
                              (max-depth *stack-depth-limit*)
                              (skip 0)
                              (offset 0))
        (do ((stack stack-trace (cdr stack))
             (skip skip (- skip 1))
             (depth offset (+ depth 1)))
            ((or (null? stack)
                 (> depth max-depth))
             (display (string-join (reverse! lines) "\n")
                      (current-error-port)))
          (and-let* (((<= skip 0))
                     (line (error-line (car stack))))
            (push! lines (format #f "~a" line))))))))

(define-class <dsm-error> ()
  ((message :init-keyword :message :accessor message-of)
   (stack-trace :init-keyword :stack-trace :accessor stack-trace-of)))

(define-method initialize ((self <dsm-error>) args)
  (next-method)
  (let-keywords* args ((stack-trace-source #f))
    (if stack-trace-source
      (set! (stack-trace-of self)
            (make-stack-trace-from-source stack-trace-source)))))

(if (symbol-bound? 'vm-get-stack-trace-lite)
  (begin
    (define (make-stack-trace-from-source src)
      (map (lambda (s)
             (if (car s)
               (let ((stack (apply extended-list (cdr s))))
                 (pair-attribute-set! stack 'source-info (car s))
                 stack)
               (cdr s)))
           src))
    (define (make-stack-trace-source st)
      (let ((result (map (lambda (s)
             (let ((info (if (pair? s)
                           (pair-attribute-get s 'source-info #f)
                           #f)))
               (cons (if (and info (pair? info) (pair? (cdr info)))
                       (cons (format "~a:~a"
                                     (sys-gethostname)
                                     (car info))
                             (cdr info))
                       #f)
                     s)))
           st)))
        '(p (map (lambda (s)
                  (if (pair? s)
                    (pair-attribute-get s 'source-info #f)
                    #f))
                st)
           (map (lambda (s)
                  (if (pair? s)
                    (pair-attribute-get s 'source-info #f)
                    #f))
                (make-stack-trace-from-source result)))
        result)))
  (begin
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
           st))))

(define-method write-object ((self <dsm-error>) out)
  (format out "#,(<dsm-error> :message ~s :stack-trace-source ~s)"
          (message-of self)
          (x->marshalized-object
           (make-stack-trace-source (stack-trace-of self))
           (make-marshal-table))))

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
    :stack-trace (if (symbol-bound? 'vm-get-stack-trace-lite)
                   (cddr (vm-get-stack-trace-lite))
                   (cdr (vm-get-stack-trace)))))

(provide "dsm/error")
