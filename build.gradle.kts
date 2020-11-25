plugins {
    `java-library`
    id("com.atlassian.performance.tools.gradle-release").version("0.7.1")
}

tasks.wrapper {
    gradleVersion = "6.7"
    distributionType = Wrapper.DistributionType.BIN
}

configurations.all {
    resolutionStrategy {
        activateDependencyLocking()
        failOnVersionConflict()
        eachDependency {
            when (requested.module.toString()) {
                "org.slf4j:slf4j-api" -> useVersion("1.7.30")
            }
        }
    }
}

dependencies {
    implementation("net.jcip:jcip-annotations:1.0")
    implementation("org.postgresql:postgresql:42.2.18")
    testImplementation("junit:junit:4.12")
    testImplementation("org.assertj:assertj-core:3.18.1")
    testImplementation("org.mockito:mockito-core:3.6.0")
    testImplementation("com.github.docker-java:docker-java-core:3.2.6")
    testImplementation("com.github.docker-java:docker-java-transport-httpclient5:3.2.6")
}

tasks.compileJava {
    options.compilerArgs.add("-Xlint:deprecation")
    options.compilerArgs.add("-Xlint:unchecked")
}

tasks.withType<Test> {
    reports {
        junitXml.isEnabled = true
    }
}

tasks.test {
    filter {
        exclude("**/*IT.class")
    }
}

val testIntegration = task<Test>("testIntegration") {
    filter {
        include("**/*IT.class")
    }
    setForkEvery(1)
    maxParallelForks = 1
}

tasks["check"].dependsOn(testIntegration)

group = "com.atlassian.db.replica"

gradleRelease {
    atlassianPrivateMode = true
}
