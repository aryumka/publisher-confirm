import org.jetbrains.kotlin.gradle.tasks.KotlinCompile

plugins {
  kotlin("jvm") version "1.7.21"
}

group = "aryumka"
version = "1.0-SNAPSHOT"

repositories {
  mavenCentral()
}

dependencies {
  testImplementation(kotlin("test"))
  implementation("com.rabbitmq:amqp-client:5.21.0")
  implementation("org.slf4j:slf4j-api:2.0.12")
  implementation("org.slf4j:slf4j-simple:2.0.12")
}

tasks.test {
  useJUnitPlatform()
}

tasks.withType<KotlinCompile> {
  kotlinOptions.jvmTarget = "1.8"
}
