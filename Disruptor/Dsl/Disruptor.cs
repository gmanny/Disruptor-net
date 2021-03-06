using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Disruptor.Dsl
{
    /// <summary>
    /// A DSL-style API for setting up the disruptor pattern around a ring buffer.
    /// </summary>
    /// <typeparam name="T">the type of event used.</typeparam>
    public class Disruptor<T> where T : class
    {
        private const bool Running = true;
        private const bool Stopped = false;

        private readonly RingBuffer<T> _ringBuffer;
        private readonly TaskScheduler _taskScheduler;
        private readonly ConsumerRepository<T> _consumerRepository = new ConsumerRepository<T>();
        private Volatile.Boolean _running = new Volatile.Boolean(Stopped);
        private readonly EventPublisher<T> _eventPublisher;
        private IExceptionHandler _exceptionHandler;

        /// <summary>
        /// Create a new Disruptor.
        /// </summary>
        /// <param name="eventFactory">the factory to create events in the ring buffer.</param>
        /// <param name="ringBufferSize">the size of the ring buffer, must be a power of 2.</param>
        /// <param name="taskScheduler">the <see cref="TaskScheduler"/> used to start <see cref="IEventProcessor"/>s.</param>
        public Disruptor(Func<T> eventFactory, int ringBufferSize, TaskScheduler taskScheduler)
            : this(new RingBuffer<T>(eventFactory, ringBufferSize), taskScheduler)
        {
        }

        /// <summary>
        /// Create a new Disruptor.
        /// </summary>
        /// <param name="eventFactory">the factory to create events in the ring buffer.</param>
        /// <param name="claimStrategy">the claim strategy to use for the ring buffer.</param>
        /// <param name="waitStrategy">the wait strategy to use for the ring buffer.</param>
        /// <param name="taskScheduler">the <see cref="TaskScheduler"/> used to start <see cref="IEventProcessor"/>s.</param>
        public Disruptor(Func<T> eventFactory, 
                         IClaimStrategy claimStrategy,
                         IWaitStrategy waitStrategy,
                         TaskScheduler taskScheduler)
            : this(new RingBuffer<T>(eventFactory, claimStrategy, waitStrategy), taskScheduler)
        {
        }

        private Disruptor(RingBuffer<T> ringBuffer, TaskScheduler taskScheduler)
        {
            if (taskScheduler == null) throw new ArgumentNullException("taskScheduler");

            _ringBuffer = ringBuffer;
            _taskScheduler = taskScheduler;
            _eventPublisher = new EventPublisher<T>(ringBuffer);
        }

        /// <summary>
        /// Set up custom <see cref="IEventProcessor"/>s to handle events from the ring buffer. The Disruptor will
        /// automatically start these processors when <see cref="Disruptor{T}.Start"/> is called.
        /// </summary>
        /// <param name="handlers">handlers the event handlers that will process events.</param>
        /// <returns>a <see cref="EventHandlerGroup{T}"/> that can be used to chain dependencies.</returns>
        public EventHandlerGroup<T> HandleEventsWith(params IEventHandler<T>[] handlers)
        {
            return CreateEventProcessors(new Sequence[0], handlers);
        }

        /// <summary>
        /// Set up custom <see cref="IEventProcessor"/> to handle events from the ring buffer. The Disruptor will
        /// automatically start those processors when <see cref="Disruptor{T}.Start"/> is called.
        /// </summary>
        /// <param name="processors">the event processors that will process events.</param>
        /// <returns>a <see cref="EventHandlerGroup{T}"/> that can be used to chain dependencies.</returns>
        public EventHandlerGroup<T> HandleEventsWith(params IEventProcessor[] processors)
        {
            foreach (var eventProcessor in processors)
            {
                _consumerRepository.Add(eventProcessor);
            }

            return new EventHandlerGroup<T>(this, _consumerRepository, Util.GetSequencesFor(processors));
        }

        /// <summary>
        /// Set up a <see cref="WorkerPool{T}"/> to distribute an event to one of a pool of work handler threads.
        /// Each event will only be processed by one of the work handlers.
        /// The Disruptor will automatically start this processors when <see cref="Disruptor{T}.Start"/> is called.
        /// </summary>
        /// <param name="workHandlers">the work handlers that will process events.</param>
        /// <returns>a <see cref="EventHandlerGroup{T}"/> that can be used to chain dependencies.</returns>
        public EventHandlerGroup<T> HandleEventsWithWorkerPool(params IWorkHandler<T>[] workHandlers)
        {
            return CreateWorkerPool(new Sequence[0], workHandlers);
        }

        /// <summary>
        /// Specify an <see cref="IExceptionHandler"/> to be used for any future event handlers.
        /// Note that only <see cref="IEventHandler{T}"/>s set up after calling this method will use the <see cref="IExceptionHandler"/>.
        /// </summary>
        /// <param name="exceptionHandler"></param>
        public void HandleExceptionsWith(IExceptionHandler exceptionHandler)
        {
            _exceptionHandler = exceptionHandler;
        }

        /// <summary>
        /// Override the default <see cref="IExceptionHandler"/> for a specific <see cref="IEventHandler{T}"/>.
        /// </summary>
        /// <param name="eventHandler"> the <see cref="IEventHandler{T}"/> to set a different <see cref="IExceptionHandler"/> for.</param>
        /// <returns>an <see cref="ExceptionHandlerSetting{T}"/> dsl object - intended to be used by chaining the with method call.</returns>
        public ExceptionHandlerSetting<T> HandleExceptionsFor(IEventHandler<T> eventHandler)
        {
            return new ExceptionHandlerSetting<T>(eventHandler, _consumerRepository);
        }

        /// <summary>
        /// Create a group of <see cref="IEventHandler{T}"/>s to be used as a dependency.
        /// </summary>
        /// <param name="handlers">the <see cref="IEventHandler{T}"/>s, previously set up with <see cref="HandleEventsWith(Disruptor.IEventHandler{T}[])"/>,
        ///                        that will form the <see cref="ISequenceBarrier"/> for subsequent handlers or processors.
        /// </param>
        /// <returns>an <see cref="EventHandlerGroup{T}"/> that can be used to setup a <see cref="ISequenceBarrier"/> over the specified <see cref="IEventHandler{T}"/>s.</returns>
        public EventHandlerGroup<T> After(params IEventHandler<T>[] handlers)
        {
            Sequence[] sequences = new Sequence[handlers.Length];
            for (int i = 0; i < handlers.Length; i++)
            {
                sequences[i] = _consumerRepository.GetSequenceFor(handlers[i]);
            }

            return new EventHandlerGroup<T>(this, _consumerRepository, sequences);
        }

        /// <summary>
        /// Create a group of <see cref="IEventProcessor"/>s to be used as a dependency.
        /// </summary>
        /// <param name="processors">the <see cref="IEventProcessor"/>s, previously set up with <see cref="HandleEventsWith(Disruptor.IEventProcessor[])"/>,
        ///                          that will form the <see cref="ISequenceBarrier"/> for subsequent <see cref="IEventHandler{T}"/> or <see cref="IEventProcessor"/>s.
        /// </param>
        /// <returns>an <see cref="EventHandlerGroup{T}"/> that can be used to setup a <see cref="ISequenceBarrier"/> over the specified <see cref="IEventProcessor"/>s.</returns>
        public EventHandlerGroup<T> After(params IEventProcessor[] processors)
        {
            foreach (var eventProcessor in processors)
            {
                _consumerRepository.Add(eventProcessor);
            }

            return new EventHandlerGroup<T>(this, _consumerRepository, Util.GetSequencesFor(processors));
        }

        /// <summary>
        /// Publish an event to the <see cref="RingBuffer"/>
        /// </summary>
        /// <param name="eventTranslator">the translator function that will load data into the event.</param>
        public void PublishEvent(Func<T, long, T> eventTranslator)
        {
            _eventPublisher.PublishEvent(eventTranslator);
        }

        /// <summary>
        /// Starts the <see cref="IEventProcessor"/>s and returns the fully configured <see cref="RingBuffer"/>.
        /// The <see cref="RingBuffer"/> is set up to prevent overwriting any entry that is yet to
        /// be processed by the slowest event processor.
        /// This method must only be called once after all <see cref="IEventProcessor"/>s have been added.
        /// </summary>
        /// <returns>the configured <see cref="RingBuffer"/>.</returns>
        public RingBuffer<T> Start()
        {
            Sequence[] gatingSequences = _consumerRepository.LastSequenceInChain;
            _ringBuffer.SetGatingSequences(gatingSequences);

            CheckOnlyStartedOnce();
            foreach (IConsumerInfo consumerInfo in _consumerRepository.EventProcessors)
            {
                consumerInfo.Start(_taskScheduler);
            }

            return _ringBuffer;
        }

        /// <summary>
        /// Calls <see cref="IEventProcessor.Halt"/> on all the <see cref="IEventProcessor"/>s created via this <see cref="Disruptor{T}"/>.
        /// </summary>
        public void Halt()
        {
            foreach (IConsumerInfo consumerInfo in _consumerRepository.EventProcessors)
            {
                consumerInfo.Halt();
            }
        }

        /// <summary>
        /// Waits until all events currently in the <see cref="Disruptor"/> have been processed by all <see cref="IEventProcessor"/>s
        /// and then halts the <see cref="IEventProcessor"/>.  It is critical that publishing to the <see cref="RingBuffer"/> has stopped
        /// before calling this method, otherwise it may never return.
        /// </summary>
        public void Shutdown()
        {
            while (HasBacklog())
            {
                Thread.Sleep(0);
            }

            Halt();
        }

        /// <summary>
        /// The the <see cref="RingBuffer"/> used by this <see cref="Disruptor{T}"/>.  This is useful for creating custom
        /// <see cref="IEventProcessor"/> if the behaviour of <see cref="BatchEventProcessor{T}"/> is not suitable.
        /// </summary>
        public RingBuffer<T> RingBuffer
        {
            get { return _ringBuffer; }
        }

        /// <summary>
        /// Get the <see cref="ISequenceBarrier"/> used by a specific handler. Note that the <see cref="ISequenceBarrier"/>
        /// may be shared by multiple event handlers.
        /// </summary>
        /// <param name="handler"></param>
        /// <returns></returns>
        public ISequenceBarrier GetBarrierFor(IEventHandler<T> handler)
        {
            return _consumerRepository.GetBarrierFor(handler);
        }

        private bool HasBacklog()
        {
            long cursor = _ringBuffer.Cursor;
            return _consumerRepository.LastSequenceInChain.Any(consumer => cursor > consumer.Value);
        }

        internal EventHandlerGroup<T> CreateEventProcessors(Sequence[] barrierSequences, IEventHandler<T>[] eventHandlers)
        {
            CheckNotStarted();

            Sequence[] processorSequences = new Sequence[eventHandlers.Length];
            ISequenceBarrier barrier = _ringBuffer.NewBarrier(barrierSequences);

            for (int i = 0; i <  eventHandlers.Length; i++)
            {
                var eventHandler = eventHandlers[i];

                var batchEventProcessor = new BatchEventProcessor<T>(_ringBuffer, barrier, eventHandler);

                if (_exceptionHandler != null)
                {
                    batchEventProcessor.SetExceptionHandler(_exceptionHandler);
                }

                _consumerRepository.Add(batchEventProcessor, eventHandler, barrier);
                processorSequences[i] = batchEventProcessor.Sequence;
            }

            if (processorSequences.Length > 0)
            {
                _consumerRepository.UnmarkEventProcessorsAsEndOfChain(barrierSequences);
            }

            return new EventHandlerGroup<T>(this, _consumerRepository, processorSequences);
        }

        internal EventHandlerGroup<T> CreateWorkerPool(Sequence[] barrierSequences, IWorkHandler<T>[] workHandlers)
        {
            ISequenceBarrier sequenceBarrier = _ringBuffer.NewBarrier(barrierSequences);
            WorkerPool<T> workerPool = new WorkerPool<T>(_ringBuffer, sequenceBarrier, _exceptionHandler, workHandlers);
            _consumerRepository.Add(workerPool, sequenceBarrier);
            return new EventHandlerGroup<T>(this, _consumerRepository, workerPool.WorkerSequences);
        }

        private void CheckNotStarted()
        {
            if (_running.ReadFullFence())
            {
                throw new InvalidOperationException("All event handlers must be added before calling starts.");
            }
        }

        private void CheckOnlyStartedOnce()
        {
            if (!_running.AtomicCompareExchange(Running, Stopped))
            {
                throw new InvalidOperationException("Disruptor.start() must only be called once.");
            }
        }
    }
}