(define-module dsm.server
  (extend dsm.dsm)
  (use srfi-13)
  (use util.queue)
  (use gauche.collection)
  (use gauche.net)
  (use gauche.threads)
  (use gauche.selector)
  (use gauche.parameter)
  (use marshal)
  (use dsm.common)
  (export make-dsm-server
          port-of path-of
          dsm-server-start! dsm-server-stop! dsm-server-join!
          add-mount-point! get-by-mount-point get-by-id
          require-in-root-thread required-in-root-thread?)
  )
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
            :init-value '(1 0))
   ))

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
  (apply make <dsm-server> (append (list :uri uri) keywords)))

(define-method add-mount-point! ((self <dsm-server>) mount-point value)
  (hash-table-put! (mount-table-of self)
                   (x->string mount-point)
                   value))

(define-method get-by-mount-point ((self <dsm-server>) mount-point)
  (let ((table (mount-table-of self))
        (key (x->string mount-point)))
    (if (hash-table-exists? table key)
      (hash-table-get table key)
      (make-dsmp-error #`"No such mount point: ,|mount-point|"))))

(define-method get-by-id ((self <dsm-server>) id)
  (id-ref (marshal-table-of self) id))

(define dsm-thread-disable #t);(eq? 'none (gauche-thread-type)))

(define-class <thread-pool> ()
  ((pool :accessor pool-of :init-form '())
   (max :accessor max-of :init-value 10 :init-keyword :max)
   (mutex :accessor mutex-of :init-form (make-mutex))
   (keeper :accessor keeper-of)))

(define-method initialize ((self <thread-pool>) args)
  (next-method)
  (set! (keeper-of self)
        (if dsm-thread-disable
          #f
          (thread-start!
           (make-thread
            (lambda ()
              (let loop ()
                (print (current-time))
                (thread-sleep! 30)
                ;; (sys-sleep 30)
                (print (current-time))
                (mutex-synchronize
                 (mutex-of self)
                 (lambda ()
                   (let ((new-pool
                          (fold (lambda (work-thread prev)
                                  (thread-join! (thread-of work-thread) 0.0 #f)
                                  (cond ((free? work-thread)
                                         (thread-terminate!
                                          (thread-of work-thread))
                                         (print "terninate")
                                         prev)
                                        (else
                                         (cons work-thread prev))))
                                '()
                                (pool-of self))))
                     (set! (pool-of self) new-pool))))
                (print (current-time))
                (loop)))
            "keeper")))))

(define-method thread-pool-join! ((self <thread-pool>) . args)
  (if (keeper-of self)
    (apply thread-join! (keeper-of self) args)))

(define-method thread-pool-push! ((self <thread-pool>) thunk)
  (if dsm-thread-disable
    (thunk)
    (let ((work-thread (thread-pool-work-thread self)))
      (add-job! work-thread thunk))))

(define-method safe-pool-of ((self <thread-pool>))
  (mutex-synchronize
   (mutex-of self)
   (lambda ()
     (pool-of self))))

(define-method thread-pool-work-thread ((self <thread-pool>))
  (let ((work-threads (sort (safe-pool-of self)
                            (lambda (x y)
                              (< (job-size x)
                                 (job-size y))))))
    (if (or (null? work-threads)
            (not (free? (car work-threads))))
      (if (< (length (pool-of self)) (max-of self))
        (let ((new-work-thread (make <work-thread>
                                 (length (safe-pool-of self)))))
          (mutex-synchronize
           (mutex-of self)
           (lambda ()
             (push! (pool-of self) new-work-thread)
             new-work-thread)))
        (if (null? work-threads)
          (error "too many thread in pool")
          (car work-threads)))
      (car work-threads))))

(define-class <work-thread> ()
  ((thread :accessor thread-of)
   (mutex :accessor mutex-of :init-form (make-mutex))
   (job-queue :accessor job-queue-of :init-form (make-queue))))

(define-method initialize ((self <work-thread>) args)
  (next-method)
  (let ((name-suffix (car args)))
    (set! (thread-of self)
          (make-thread
           (lambda ()
             (let loop ()
               (do-next-job! self)
               (thread-yield!)
               (loop)))
           #`"work-thread-,|name-suffix|"))
    (thread-specific-set! (thread-of self) (job-queue-of self)))
  (thread-start! (thread-of self)))
  
(define-method free? ((self <work-thread>))
  (mutex-synchronize
   (mutex-of self)
   (lambda ()
     (unsafe-free? self))))

(define-method unsafe-free? ((self <work-thread>))
  (queue-empty? (job-queue-of self)))

(define-method job-size ((self <work-thread>))
  (mutex-synchronize
   (mutex-of self)
   (lambda ()
     (queue-length (job-queue-of self)))))

(define-method add-job! ((self <work-thread>) thunk)
  (mutex-synchronize
   (mutex-of self)
   (lambda ()
     (enqueue! (job-queue-of self) thunk))))

(define-method next-job! ((self <work-thread>))
  (dequeue! (job-queue-of self)))

(use gauche.interactive)
(define-method do-next-job! ((self <work-thread>))
  (mutex-synchronize
   (mutex-of self)
   (lambda ()
     (unless (unsafe-free? self)
       ((next-job! self))))))
   
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
(define (dsm-server-join! server)
  (if (thread-of server)
    (let ((sym (gensym)))
      (let loop ()
        (with-error-handler
            (lambda (e)
              (if (uncaught-exception? e)
                (p (ref (uncaught-exception-reason e) 'message))
                (p (ref e 'message)))
              (raise e))
          (lambda ()
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
        ;; (p (socket-fd client))
        (selector-add! selector
                       (socket-input-port client :buffering :modest)
                       (lambda (input flag)
                         (handle-dsm-protocol client input output))
                       '(r))))

    (define (handle-dsm-protocol client input output)
      (let ((mutex (dsm-thread-mutex-get client)))
        (thread-pool-push!
         (thread-pool-of self)
         (lambda ()
           (mutex-synchronize
            mutex
            (lambda ()
              (call/cc
               (lambda (cont)
                 (dsm-response (protocol-of self)
                               (marshal-table-of self)
                               input output
                               :get-handler (cut get-by-mount-point self <>)
                               :eof-handler (cut (make-eof-handler cont)
                                                 client input output))))))))))

    (define (make-eof-handler return)
      (lambda (client input output)
        (selector-delete! selector input #f #f)
        (socket-shutdown client 2)
        ;; (p "finished!")
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
