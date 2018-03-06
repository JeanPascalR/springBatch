package consulting.jjs.batch.dto;

public class Person {
  private int age;
  private String name, email;

  public Person() {};

  public Person(int age, String name, String email) {
    this.age = age;
    this.name = name;
    this.email = email;
  }

  public int getAge() {
    return age;
  }

  public void setAge(int age) {
    this.age = age;
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public String getEmail() {
    return email;
  }

  public void setEmail(String email) {
    this.email = email;
  }
}
