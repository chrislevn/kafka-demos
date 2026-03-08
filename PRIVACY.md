# Privacy Policy

**Last Updated:** March 8, 2026

## 1. Introduction

This Privacy Policy describes how the Kafka Ordering Demos project ("the Software") handles information. We are committed to transparency about data practices associated with this project.

## 2. Information We Do Not Collect

The Software is a self-hosted educational demo that runs entirely on your local machine. **We do not collect, store, transmit, or process any personal data.** Specifically:

- We do not collect analytics or telemetry.
- We do not use cookies or tracking technologies.
- We do not collect IP addresses, usage data, or behavioral data.
- We do not have any server-side components that receive your data.

## 3. Data Generated During Use

When you run the Software locally, it generates simulated e-commerce order data (e.g., `ORD-001`, `CREATED`, `PAID`) within your local Apache Kafka instance. This data:

- Is entirely fictional and generated programmatically.
- Exists only on your local machine within your Docker containers.
- Is deleted when you run `docker compose down -v`.
- Is never transmitted to any external service by the Software.

## 4. Third-Party Services

The Software uses the following third-party Docker images and services that run locally on your machine:

| Service | Image | Purpose |
|---------|-------|---------|
| Zookeeper | `confluentinc/cp-zookeeper` | Kafka coordination |
| Kafka | `confluentinc/cp-kafka` | Message broker |
| Kafka UI | `provectuslabs/kafka-ui` | Web-based Kafka dashboard |

These images are pulled from Docker Hub. Docker Hub's own [privacy policy](https://www.docker.com/legal/docker-privacy-policy/) applies to the act of downloading these images. Once running locally, these services do not transmit data externally.

## 5. GitHub

If you interact with this project on GitHub (e.g., starring, forking, opening issues, or submitting pull requests), GitHub's [Privacy Statement](https://docs.github.com/en/site-policy/privacy-policies/github-general-privacy-statement) governs the collection and use of your data by GitHub.

## 6. Your Responsibilities

If you modify the Software to process real or personal data, you are solely responsible for:

- Complying with applicable data protection laws (e.g., GDPR, CCPA).
- Implementing appropriate security measures.
- Providing notice and obtaining consent where required.

## 7. Children's Privacy

The Software does not knowingly collect or solicit information from anyone under the age of 13.

## 8. Changes to This Policy

We may update this Privacy Policy from time to time. Changes will be reflected by updating the "Last Updated" date above.

## 9. Contact

For questions about this Privacy Policy, please open an issue on the [GitHub repository](https://github.com/chrislevn/kafka-demos).
