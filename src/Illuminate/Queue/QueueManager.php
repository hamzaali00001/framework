<?php

namespace Illuminate\Queue;

use Closure;
use Illuminate\Support\Manager;
use InvalidArgumentException;
use Illuminate\Contracts\Queue\Factory as FactoryContract;
use Illuminate\Contracts\Queue\Monitor as MonitorContract;

/**
 * @mixin \Illuminate\Contracts\Queue\Queue
 */
class QueueManager extends Manager implements FactoryContract, MonitorContract
{
    /**
     * The array of resolved queue connections.
     *
     * @var array
     */
    protected $connections = [];

    /**
     * The array of resolved queue connectors.
     *
     * @var array
     */
    protected $connectors = [];

    /**
     * Register an event listener for the before job event.
     *
     * @param  mixed  $callback
     * @return void
     */
    public function before($callback)
    {
        $this->registerEvent(Events\JobProcessing::class, $callback);
    }

    /**
     * Register an event listener for the after job event.
     *
     * @param  mixed  $callback
     * @return void
     */
    public function after($callback)
    {
        $this->registerEvent(Events\JobProcessed::class, $callback);
    }

    /**
     * Register an event listener for the exception occurred job event.
     *
     * @param  mixed  $callback
     * @return void
     */
    public function exceptionOccurred($callback)
    {
        $this->registerEvent(Events\JobExceptionOccurred::class, $callback);
    }

    /**
     * Register an event listener for the daemon queue loop.
     *
     * @param  mixed  $callback
     * @return void
     */
    public function looping($callback)
    {
        $this->registerEvent(Events\Looping::class, $callback);
    }

    /**
     * Register an event listener for the failed job event.
     *
     * @param  mixed  $callback
     * @return void
     */
    public function failing($callback)
    {
        $this->registerEvent(Events\JobFailed::class, $callback);
    }

    /**
     * Register an event listener for the daemon queue stopping.
     *
     * @param  mixed  $callback
     * @return void
     */
    public function stopping($callback)
    {
        $this->registerEvent(Events\WorkerStopping::class, $callback);
    }

    /**
     * Register an event listener.
     *
     * @param $class
     * @param  mixed  $callback
     * @return void
     */
    protected function registerEvent($class, $callback)
    {
        $this->app['events']->listen($class, $callback);
    }

    /**
     * Determine if the driver is connected.
     *
     * @param  string  $name
     * @return bool
     */
    public function connected($name = null)
    {
        return isset($this->connections[$name ?: $this->getDefaultDriver()]);
    }

    /**
     * Resolve a queue connection instance.
     *
     * @param  string  $name
     * @return \Illuminate\Contracts\Queue\Queue
     */
    public function connection($name = null)
    {
        $name = $name ?: $this->getDefaultDriver();

        // If the connection has not been resolved yet we will resolve it now as all
        // of the connections are resolved when they are actually needed so we do
        // not make any unnecessary connection to the various queue end-points.
        $connection = &$this->connections[$name];
        if (! isset($connection)) {
            $connection = $this->resolve($name);

            $connection->setContainer($this->app);
        }

        return $connection;
    }

    /**
     * Resolve a queue connection.
     *
     * @param  string  $name
     * @return \Illuminate\Contracts\Queue\Queue
     */
    protected function resolve($name)
    {
        $config = $this->getConfig($name);

        return $this->getConnector($config['driver'])
                        ->connect($config)
                        ->setConnectionName($name);
    }

    /**
     * Get the connector for a given driver.
     *
     * @param  string  $driver
     * @return \Illuminate\Queue\Connectors\ConnectorInterface
     *
     * @throws \InvalidArgumentException
     */
    protected function getConnector($driver)
    {
        if (! isset($this->connectors[$driver])) {
            throw new InvalidArgumentException("No connector for [$driver]");
        }

        return call_user_func($this->connectors[$driver]);
    }

    /**
     * Add a queue connection resolver.
     *
     * @param  string    $driver
     * @param  \Closure  $resolver
     * @return void
     */
    public function extend($driver, Closure $resolver)
    {
        $this->addConnector($driver, $resolver);
    }

    /**
     * Add a queue connection resolver.
     *
     * @param  string    $driver
     * @param  \Closure  $resolver
     * @return void
     */
    public function addConnector($driver, Closure $resolver)
    {
        $this->connectors[$driver] = $resolver;
    }

    /**
     * Get the queue connection configuration.
     *
     * @param  string  $name
     * @return array
     */
    protected function getConfig($name)
    {
        if (! is_null($name) && $name !== 'null') {
            return $this->app['config']["queue.connections.{$name}"];
        }

        return ['driver' => 'null'];
    }

    /**
     * Get the name of the default queue connection.
     *
     * @return string
     */
    public function getDefaultDriver()
    {
        return $this->app['config']['queue.default'];
    }

    /**
     * Set the name of the default queue connection.
     *
     * @param  string  $name
     * @return void
     */
    public function setDefaultDriver($name)
    {
        $this->app['config']['queue.default'] = $name;
    }

    /**
     * Get the full name for the given connection.
     *
     * @param  string  $connection
     * @return string
     */
    public function getName($connection = null)
    {
        return $connection ?: $this->getDefaultDriver();
    }

    /**
     * Determine if the application is in maintenance mode.
     *
     * @return bool
     */
    public function isDownForMaintenance()
    {
        return $this->app->isDownForMaintenance();
    }

    /**
     * Dynamically pass calls to the default connection.
     *
     * @param  string  $method
     * @param  array   $parameters
     * @return mixed
     */
    public function __call($method, $parameters)
    {
        return $this->connection()->$method(...$parameters);
    }
}
