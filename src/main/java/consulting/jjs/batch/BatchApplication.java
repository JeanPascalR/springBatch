package consulting.jjs.batch;

import consulting.jjs.batch.dto.Person;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecutionListener;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.database.builder.JdbcBatchItemWriterBuilder;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.core.io.Resource;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.core.task.TaskExecutor;

import javax.sql.DataSource;

@EnableBatchProcessing
@SpringBootApplication
public class BatchApplication {

  private static final Logger log = LoggerFactory.getLogger(BatchApplication.class);

  public static void main(String[] args) {
    SpringApplication.run(BatchApplication.class, args);
  }

  @Bean
  FlatFileItemReader<Person> flatFileItemReader(@Value("${input.file.url}") Resource in) throws Exception {
    return new FlatFileItemReaderBuilder<Person>()
            .resource(in)
            .name("file-reader")
            .targetType(Person.class)
            .delimited().delimiter(",").names(new String[]{"name", "age", "email"})
            .build();
  }

  @Bean
  JdbcBatchItemWriter<Person> jdbcWriter(DataSource dataSource) {
    return new JdbcBatchItemWriterBuilder<Person>()
            .dataSource(dataSource)
            .sql("INSERT INTO PEOPLE (AGE, FIRST_NAME, EMAIL) VALUES (:age, :name, :email)")
            .beanMapped()
            .build();
  }

  @Bean
  public TaskExecutor taskExecutor(){
    return new SimpleAsyncTaskExecutor("spring_batch_async_exec");
  }

  @Bean
  public Step file2dbStep(StepBuilderFactory sbf, ItemReader<? extends Person> ir, ItemWriter<? super Person> iw,
                          TaskExecutor taskExecutor) {
    return sbf.get("file-db")
            .<Person, Person>chunk(100)
            .reader(ir)
            .writer(iw)
            //.taskExecutor(taskExecutor)
            //.throttleLimit(5)
            .build();
  }

  @Bean
  Job job(JobBuilderFactory jbf, JobExecutionListener jobExecutionListener, Step step) {
    return jbf.get("etl")
            .incrementer(new RunIdIncrementer())
            .listener(jobExecutionListener)
            .start(step)
            .build();
  }

  //@Bean
  /*public CommandLineRunner demo(CustomerRepository repository) {
    for (Customer customer : repository.findAll()) {
      log.info("###### BEFORE {}", customer);
    }

    return (args) -> {
      // save a couple of customers
      Customer daCustomer = new Customer("Jack", "Bauer");
      log.info("A client : {}", daCustomer);
      repository.save(daCustomer);
      log.info("B client : {}", daCustomer);
      repository.save(new Customer("Chloe", "O'Brian"));
      repository.save(new Customer("Kim", "Bauer"));
      repository.save(new Customer("David", "Palmer"));
      repository.save(new Customer("Michelle", "Dessler"));

      // fetch all customers
      log.info("Customers found with findAll():");
      log.info("-------------------------------");
      for (Customer customer : repository.findAll()) {
        log.info(customer.toString());
      }
      log.info("");

      // fetch an individual customer by ID
      Customer customer = repository.findOne(1L);
      log.info("Customer found with findOne(1L):");
      log.info("--------------------------------");
      log.info(customer.toString());
      log.info("");

      // fetch customers by last name
      log.info("Customer found with findByLastName('Bauer'):");
      log.info("--------------------------------------------");
      for (Customer bauer : repository.findByLastName("Bauer")) {
        log.info(bauer.toString());
      }
      log.info("");
    };
  }*/

}
