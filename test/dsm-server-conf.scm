(define marshalizable-key&value-alist
  ; (mount-point . value)
  '(("integer" . 1)
    ("string" . "str")
    ("symbol" . sym)
    ("list" . (1 "str" sym))
    ("vector" . #(1 "str" sym))
    ))

(define procedure-list
  ; (mount-point proc expected arg ...)
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
    ))
