(define-module dsm.server
  (extend dsm.dsm)
  (use srfi-1)
  (use srfi-13)
  (use util.queue)
  (use gauche.collection)
  (use gauche.net)
  (use gauche.threads)
  (use gauche.selector)
  (use gauche.parameter)
  (use msm.marshal)
  (use dsm.common)
  (export make-dsm-server
          port-of path-of uri-of
          dsm-server-start! dsm-server-stop! dsm-server-join!
          add-mount-point! get-by-mount-point get-by-id
          require-in-root-thread required-in-root-thread?))
(select-module dsm.server)

(define-class <dsm-server> ()
  ((uri :init-keyword :uri :accessor uri-of)
   (socket :accessor socket-of)
   (protocol :accessor protocol-of)
   (thread :accessor thread-of)
   (thread-pool :accessor thread-pool-of
                :init-form (make <thread-pool>))
   (mount-table :accessor mount-table-of
                :init-form (make-hash-table 'string=?))
   (marshal-table :accessor marshal-table-of
                  :init-form (make-marshal-table))
   (timeout :init-keyword :timeout :accessor timeout-of
            :init-value '(0 500000))))

(define-method initialize ((self <dsm-server>) args)
  (next-method)
  (receive (scheme user-info host port path query fragment)
      (parse-uri (uri-of self))
    (unless scheme
      (error "scheme isn't specified."))
    (set! (socket-of self)
          (apply make-server-socket
                 (case scheme
                   ((dsmp http) (list 'inet (or port 0) :reuse-addr? #t))
                   ((unix) (list 'unix path))
                   (else (error "unknown scheme: " scheme)))))
    (set! (protocol-of self)
          (case scheme
            ((dsmp unix) (make <dsmp>))
            ((http) (make <dsmp-over-http>))
            (else (error "unknown scheme: " scheme))))))

(define-method port-of ((self <dsm-server>))
  (let ((addr (socket-address (socket-of self))))
    (if (eq? 'unix (sockaddr-family))
      #f
      (sockaddr-port addr))))

(define-method path-of ((self <dsm-server>))
  (let ((addr (socket-address (socket-of self))))
    (if (eq? 'unix (sockaddr-family))
      (sockaddr-name addr)
      #f)))

(define (make-dsm-server uri . keywords)
  (apply make <dsm-server> :uri uri keywords))

(define-method add-mount-point! ((self <dsm-server>) mount-point value)
  (hash-table-put! (mount-table-of self)
                   (x->string mount-point)
                   value))

(define-method get-by-mount-point ((self <dsm-server>) mount-point)
  (let ((table (mount-table-of self))
        (key (x->string mount-point)))
    (if (hash-table-exists? table key)
      (hash-table-get table key)
      (make-dsm-error #`"No such mount point: ,|mount-point|"))))

(define-method get-by-id ((self <dsm-server>) id)
  (id-ref (marshal-table-of self) id))

(define dsm-thread-disable #t);(eq? 'none (gauche-thread-type)))

(define-class <thread-pool> ()
  ((pool :accessor pool-of :init-form '())
   (max :accessor max-of :init-value 10 :init-keyword :max)
   (mutex :accessor mutex-of :init-form (make-mutex))
   (job-queue :accessor job-queue-of :init-form (make-queue))
   (dirty :accessor dirty-of :init-value 0)
   (keeper :accessor keeper-of)
   (mark :accessor mark-of :init-form (gensym))))

(define (thread-join-with-error-handling thunk)
  (with-error-handler
      (lambda (e)
        ;; (p e)
        (raise (if (uncaught-exception? e)
                 (uncaught-exception-reason e)
                 e)))
    thunk))

(define-method initialize ((self <thread-pool>) args)
  (next-method)
  (set! (keeper-of self)
        (if dsm-thread-disable
          #f
          (thread-start!
           (make-thread
            (lambda ()
              (let loop ()
                (thread-sleep! 10)
                ;; (p (length (pool-of self)))
                ;; (p (count free? (pool-of self)))
                ;; (p (queue-length (job-queue-of self)))
                (update-pool! self)
                (thread-yield!)
                (loop)))
            "keeper")))))

(define-method update-pool! ((self <thread-pool>))
  (mutex-synchronize
   (mutex-of self)
   (lambda ()
     ;; (p "COLLECTING...")
     (set! (pool-of self)
           (fold (lambda (work-thread prev)
                   ;; (p (thread-of work-thread))
                   (if (eq? (mark-of self)
                            (with-error-handler
                                (lambda (e)
                                  (report-error e)
                                  #f)
                              (lambda ()
                                (thread-join-with-error-handling
                                 (lambda ()
                                   (thread-join! (thread-of work-thread)
                                                 0.0 (mark-of self)))))))
                     (cons work-thread prev)
                     (begin
                       ;; (print "terminate")
                       prev)))
                 '()
                 (pool-of self)))))
    (if (have-job? self)
      (add-work-thread-if-need self)))

(define-method thread-pool-join! ((self <thread-pool>) . args)
  (if (keeper-of self)
    (apply thread-join! (keeper-of self) args)))

(define-method dirty! ((self <thread-pool>))
  (mutex-synchronize
   (mutex-of self)
   (lambda ()
     (inc! (dirty-of self)))))

(define-method reset-dirty! ((self <thread-pool>))
  (mutex-synchronize
   (mutex-of self)
   (lambda ()
     (set! (dirty-of self) 0))))

(define-method too-dirty? ((self <thread-pool>))
  (> (dirty-of self)
     (/ (max-of self) 2)))

(define-method thread-pool-push! ((self <thread-pool>) thunk)
  (if dsm-thread-disable
    (thunk)
    (begin
      (dirty! self)
      (add-work-thread-if-need self)
      (do ()
          ((< (job-size self)
              (* 10 (max-of self))))
        (print "Too many jobs; "
                #`"jobs:,(job-size self) "
                #`"pool:,(length (pool-of self))")
        (update-pool! self)
        (thread-yield!)
        (thread-sleep! 5))
      ;; (thunk)
      (add-job! self thunk)
      (add-work-thread-if-need self)
      (when (too-dirty? self)
        ;; (p "DIRTY!!")
        (reset-dirty! self)
        (thread-yield!)))))

(define-method safe-pool-of ((self <thread-pool>))
  (mutex-synchronize
   (mutex-of self)
   (lambda ()
     (pool-of self))))

(define-method job-size ((self <thread-pool>))
  (mutex-synchronize
   (mutex-of self)
   (lambda ()
     (queue-length (job-queue-of self)))))

(define-method have-job? ((self <thread-pool>))
  (mutex-synchronize
   (mutex-of self)
   (lambda ()
     (not (queue-empty? (job-queue-of self))))))

(define-method next-job! ((self <thread-pool>))
  (mutex-synchronize
   (mutex-of self)
   (lambda ()
     (if (queue-empty? (job-queue-of self))
       #f
       (dequeue! (job-queue-of self))))))

(define-method add-job! ((self <thread-pool>) thunk)
  (mutex-synchronize
   (mutex-of self)
   (lambda ()
     ;; (p "NEW JOB!!")
     (enqueue! (job-queue-of self) thunk))))

(define-method add-work-thread-if-need ((self <thread-pool>))
  (if (or (have-job? self)
          (null? (safe-pool-of self)))
    (let ((pool-size (length (safe-pool-of self))))
      (if (< pool-size (max-of self))
        (let ((new-work-thread (make <work-thread>
                                 :pool self
                                 :name (number->string pool-size))))
          (mutex-synchronize
           (mutex-of self)
           (lambda ()
             ;; (p "ADDED!!!")
             (push! (pool-of self) new-work-thread))))))))

(define-class <work-thread> ()
  ((name :accessor name-of :init-keyword :name)
   (pool :accessor pool-of :init-keyword :pool)
   (thread :accessor thread-of)
   (mutex :accessor mutex-of :init-form (make-mutex))))

(define-method initialize ((self <work-thread>) args)
  (next-method)
  (set! (thread-of self)
        (make-thread
         (lambda ()
           (call/cc
            (lambda (cont)
              (let loop ((count 10))
                (if (do-next-job self cont)
                  (begin
                    (thread-yield!)
                    (loop (- count 1)))
                  (if (< count 0)
                    (begin
                      ;; (p "TIRED!!!")
                      #f)
                    (begin
                      (thread-sleep! 0.1)
                      (loop count))))))))
         #`"work-thread-,(name-of self)"))
  (set-working! self #f)
  (thread-start! (thread-of self)))

(define-method set-working! ((self <work-thread>) bool)
  (mutex-synchronize
   (mutex-of self)
   (lambda ()
     (thread-specific-set! (thread-of self) bool))))

(define-method working? ((self <work-thread>))
  (thread-specific (thread-of self)))

(define-method free? ((self <work-thread>))
  (not (working? self)))

(use gauche.interactive)

(define-method do-next-job ((self <work-thread>) return)
  (and (have-job? (pool-of self))
       (let ((job (next-job! (pool-of self))))
         (cond ((procedure? job)
                (set-working! self #t)
                ;; (p "DO")
                (job)
                ;; (p "FINISHED")
                (set-working! self #f)
                #t)
               (else
                (set-working! self #f)
                (return #f))))))

(define (mutex-synchronize mutex thunk)
  (dynamic-wind
    (lambda () (mutex-lock! mutex))
    thunk
    (lambda () (mutex-unlock! mutex))))

(define (mutex-synchronize* mutex thunk)
  (thunk))

(define dsm-thread-mutex-table-mutex (make-mutex))
(define dsm-thread-mutex-table (make-hash-table 'equal?))

(define (dsm-thread-mutex-get socket)
  (mutex-synchronize
   dsm-thread-mutex-table-mutex
   (lambda ()
     (let* ((key (socket-fd socket))
            (mutex (hash-table-get dsm-thread-mutex-table key #f)))
         (if mutex
           mutex
           (begin
             (let ((mutex (make-mutex)))
               (hash-table-put! dsm-thread-mutex-table key mutex)
               mutex)))))))

(define (dsm-thread-mutex-delete! socket)
  (mutex-synchronize
   dsm-thread-mutex-table-mutex
   (lambda ()
     (let* ((key (socket-fd socket-fd))
            (mutex (hash-table-get dsm-thread-mutex-table key)))
       (hash-table-delete! dsm-thread-mutex-table key)))))

(define dsm-cv (make-parameter (make-condition-variable)))
(define dsm-mu (make-parameter (make-mutex)))
(define (require-in-root-thread path mod)
  (if dsm-thread-disable
    (begin
      (eval `(require ,path) mod)
      (condition-variable-specific-set! (dsm-cv) #t))
    (begin
      (mutex-synchronize
       (dsm-mu)
       (lambda ()
         (condition-variable-specific-set! (dsm-cv) (cons path mod))))
      (condition-variable-signal! (dsm-cv))
      (thread-yield!))))

(define (required-in-root-thread?)
  (eq? #t (condition-variable-specific (dsm-cv))))

(define (reset-required-in-root-thread)
  (condition-variable-specific-set! (dsm-cv) #f))

(define (set-dsm-server-thread! server thunk)
  (cond (dsm-thread-disable
         (set! (thread-of server) #f)
         (thunk))
        (else
         (set! (thread-of server) (make-thread thunk "dsm-server-thread"))
         (thread-start! (thread-of server)))))

(use gauche.interactive)
'(define (p . args)
  #f)

(define (dsm-server-join! server)
  (if (thread-of server)
    (let ((sym (gensym)))
      (let loop ()
        (thread-join-with-error-handling
         (lambda ()
           ;; (p "dsm-server")
           (if (and (eq? sym (thread-join! (thread-of server) 1.0 sym))
                    (eq? sym (thread-pool-join! (thread-pool-of server)
                                                0.0 sym)))
             (loop))))))))

(define-method dsm-server-start! ((self <dsm-server>) . args)
  (let ((selector (make <selector>))
        (handler (get-optional args #f)))
    
    (define (accept-handler sock flag)
      (let* ((client (socket-accept (socket-of self)))
             (output (socket-output-port client :buffering :full)))
        (handle-dsm-protocol client
                             (socket-input-port client :buffering :modest)
                             output)))

    (define (handle-dsm-protocol client input output)
      (thread-pool-push!
       (thread-pool-of self)
       (lambda ()
         (call/cc
          (lambda (cont)
            (dsm-response (protocol-of self)
                          (marshal-table-of self)
                          input output
                          :get-handler (cut get-by-mount-point self <>)
                          :eof-handler (cut (make-eof-handler cont)
                                            client input output)
                          :not-response-handler
                          (lambda ()
                            (print "not resposne from client")
                            (cont 0))
                          :timeout #f)
            ;; (p "SELECTOR")
            (selector-add! selector
                           input
                           (lambda (in flag)
                             (selector-delete! selector in #f #f)
                             (handle-dsm-protocol client in output))
                           '(r)))))))

    (define (make-eof-handler return)
      (lambda (client input output)
        ;; (p "finished!")
        (selector-delete! selector input #f #f)
        (socket-shutdown client 2)
        ;; (p "clean!")
        (return 0)))
    
    (selector-add! selector
                   (socket-fd (socket-of self))
                   accept-handler
                   '(r))
    (parameterize ((dsm-cv (make-condition-variable))
                   (dsm-mu (make-mutex)))
      (reset-required-in-root-thread)
      (set-dsm-server-thread!
       self
       (lambda ()
         (do () ((eq? 'shutdown (socket-status (socket-of self))))
           (if (mutex-synchronize
                (dsm-mu)
                (lambda ()
                  (if (and (pair? (condition-variable-specific (dsm-cv)))
                           (not dsm-thread-disable))
                    (begin
                      ;; (p (current-thread) (condition-variable-specific (dsm-cv)))
                      ;; (load (condition-variable-specific (dsm-cv)))
                      (eval `(require ,(car (condition-variable-specific (dsm-cv))))
                            (cdr (condition-variable-specific (dsm-cv))))
                      (condition-variable-specific-set! (dsm-cv) #t)
                      #t)
                    #f)))
             (thread-yield!))
           (if handler
             (handler self))
           (selector-select selector (timeout-of self))))))))

(define-method dsm-server-stop! ((self <dsm-server>))
  (socket-close (socket-of self))
  (socket-shutdown (socket-of self) 2))

(provide "dsm/server")
