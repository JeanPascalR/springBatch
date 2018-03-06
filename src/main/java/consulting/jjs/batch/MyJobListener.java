package consulting.jjs.batch;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobExecutionListener;
import org.springframework.stereotype.Component;

@Component
public class MyJobListener implements JobExecutionListener {

  private static final Logger LOGGER = LoggerFactory.getLogger(MyJobListener.class);

  private long startTime;

  @Override
  public void beforeJob(JobExecution jobExecution) {
    startTime = System.nanoTime();
  }

  @Override
  public void afterJob(JobExecution jobExecution) {
    LOGGER.info("###### Job duration: " + ((System.nanoTime() - startTime) / 1e6 + "ms."));
  }

}
