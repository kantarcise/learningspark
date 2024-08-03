## Wait, what even is an Option type? 🤔

In Scala, the `Option` type is used to represent a value that may or may not be present. It's a container type that can either hold a value (`Some(value)`) or no value (`None`). This is particularly useful for handling optional or nullable values in a type-safe manner.

### What is the `Option` Type?

The `Option` type is a sealed abstract class with two subclasses:

1. `Some[T]`: Represents an `Option` that contains a value of type `T`.
2. `None`: Represents an `Option` that contains no value.

The primary purpose of the `Option` type is to avoid the use of null values, which can lead to `NullPointerException`s and make the code harder to understand and maintain.

### Why Do We Use the `Option` Type?

Using `Option` helps to:
1. **Express Intent Clearly**: It makes it explicit in the type system that a value may or may not be present.
2. **Avoid Nulls**: It provides a safer alternative to using `null`, which is common in languages like Java.
3. **Pattern Matching**: It allows the use of pattern matching to handle both cases (`Some` and `None`) cleanly.

### What is `Some`?

`Some` is a case class that is used to represent an `Option` that contains a value. For example, `Some(5)` is an `Option[Int]` that contains the value 5.

### Why Did We Use `Some` in the Test Data?

When you use `Option[Datatype]` in a case class, the instances of that case class may or may not have values for the fields of type `Option`. In your unit tests, if you want to mock data where the optional fields have values, you use `Some(Datatype)`. This indicates that the field has a specific value, rather than being absent (`None`).

### Example and Relationship

Here is an example to illustrate the relationship between `Option`, `Some`, and `None`:

```scala
// Define a case class with Option types
case class User(name: String, age: Option[Int])

// Creating instances of the User case class
val user1 = User("Alice", Some(25))  // User with an age
val user2 = User("Bob", None)        // User without an age

// Accessing the age field using pattern matching
user1.age match {
  case Some(a) => println(s"Alice is $a years old")
  case None => println("Alice's age is not available")
}

user2.age match {
  case Some(a) => println(s"Bob is $a years old")
  case None => println("Bob's age is not available")
}
```

In the example above:
- `User("Alice", Some(25))` means Alice has an age of 25.
- `User("Bob", None)` means Bob's age is not available.

When writing unit tests, if you want to specify that a user has an age, you use `Some` to wrap the value:

```scala
// Mock data for unit tests
val testUser1 = User("TestUser1", Some(30))
val testUser2 = User("TestUser2", None)

// Assertions
assert(testUser1.age == Some(30))
assert(testUser2.age == None)
```

Here, `Some(30)` indicates that `testUser1` has an age of 30, while `None` indicates that `testUser2` does not have an age.

By using `Option` and its subclasses `Some` and `None`, you can write more expressive, type-safe, and null-safe Scala code.

Thanks to GPT-4o.