// Import statements for required classes and libraries
package org.ashwin.hodoor;
import java.util.Scanner;
import java.util.concurrent.TimeUnit;
import org.junit.Test;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import com.codahale.metrics.ConsoleReporter;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.codahale.metrics.Timer.Context;
import redis.clients.jedis.JedisPool;

// Definition of the HodoorTest class
public class HodoorTest {
    // MetricRegistry is used to collect metrics (e.g., timers, meters)
    private static final MetricRegistry metrics = new MetricRegistry();

    // ConsoleReporter is used to report metrics to the console
    private static ConsoleReporter reporter = ConsoleReporter.forRegistry(metrics).build();

    // Meter to measure the number of successful requests
    private static final Meter requests = metrics.meter(MetricRegistry.name(HodoorTest.class, "success"));

    // Timer to measure the total time taken for requests
    private Timer timer = metrics.timer(MetricRegistry.name(HodoorTest.class, "totalRequest"));

    // JUnit test method
    @Test
    public void testRedisRateLimit() throws InterruptedException {
        // Start reporting metrics to the console every 10 seconds
        reporter.start(10, TimeUnit.SECONDS);

        // Create an ApplicationContext based on the Spring configuration in "root-context.xml"
        ApplicationContext ac = new ClassPathXmlApplicationContext("root-context.xml");

        // Define a Runnable task for rate limiting tests
        Runnable runnable = () -> {
            // Retrieve a JedisPool bean from the Spring ApplicationContext
            JedisPool pool = (JedisPool) ac.getBean("jedisPool");

            // Create a Hodoor rate limiter with a limit of 120 requests per minute
            Hodoor rateLimiter = new Hodoor(pool, TimeUnit.MINUTES, 120);

            while (true) {
                boolean flag = false;

                // Start measuring time for a request
                Context context = timer.time();

                // Acquire a permit from the rate limiter
                if(rateLimiter.acquire("testMKey1")) {
                    flag = true;
                }

                // Stop measuring time for the request
                context.stop();

                // If the request was successful, mark it in the "requests" meter
                if (flag) {
                    requests.mark();
                }

                try {
                    // Sleep for 1 millisecond before making the next request
                    Thread.sleep(1);
                }
                catch(Exception e) {
                    // Handle any exceptions that occur during sleep
                }
            }
        };

        // Number of threads for concurrent rate limiting tests
        int threadCount = 10;

        // Create and start multiple threads to simulate concurrent requests
        for(int i = 0; i < threadCount; i++ ) {
            Thread t = new Thread(runnable);
            t.start();
        }

        // Wait for user input before exiting the test
        Scanner scanner = new Scanner(System.in);
        scanner.nextLine();
    }
}
