(define marshallable-key&value-list
  ;; (mount-point . value)
  '(("integer" 1)
    ("string" "str")
    ("symbol" sym)
    ("list" (1 "str" sym))
    ("vector" #(1 "str" sym))))

(define procedure-list
  ;; (mount-point proc expected arg ...)
  `(("procedure0" ,(lambda () #t) #t)
    ("procedure1" ,(lambda (x) (+ x 2)) 3 1)
    ("procedure2" ,(lambda (x y) (+ x y)) 3 1 2)
    ("procedure3" ,(lambda (proc x) (proc x)) 3 ,(lambda (x) (+ 1 x)) 2)
    ("procedure4" ,(lambda (proc x)
                     (proc (lambda (y) (+ y 5))
                           x))
                  8
                  ,(lambda (proc x)
                     (proc (+ 1 x)))
                  2)
    ("procedure5" ,map (10 20) ,(lambda (x) (* 10 x)) (1 2))
    ("procedure6" ,(lambda (procs . args)
                     (apply map apply procs args))
                   (6 120)
                   (,+ ,*)
                   ((1 2 3)
                    (4 5 6)))))

(define procedure-in-collection-list
  ;; (mount-point proc-in-collection expected (arg ...) ...)
  `(("proc-in-col" (,(lambda () #t)) (#t) ())))

(define procedure-in-collection2-list
  ;; (mount-point proc-in-collection-in-collection expected (arg ...) ...)
  `(("proc-in-col2" ((dummy ,(lambda () #t))) (#t) ())))

(define error-procedure-list
  ; (mount-point proc-which-couse-error arg ...)
  `(("error-proc1" ,(lambda () (1))
                   #/^invalid application: \(1\)/)
    ("error-proc2" ,(lambda (procs . args) 
                      (apply map apply procs args))
                   #/^operation \+ is not defined between \(1\) and \(2\)/
                   (,+ ,*)
                   (((1) (2) (3))
                    ((4) (5) (6))))))
