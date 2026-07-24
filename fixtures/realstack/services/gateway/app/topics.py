"""Topic-name StrEnum (M7 T2, OPEN R2a enum fan-out leg): the family of document
topics TopicMirror.replicate can target at runtime -- WHICH member a given call
sends to is runtime data (an argument), statically unresolvable, so the producer
idiom (workspace.yaml's doc-topic-mirror) declares the whole enum via
`name_from: {enum: "app.topics.DocTopicName"}` and kafka_ext fans one PRODUCES
edge out PER member at a fixed heuristic/0.8 (documented over-approximation).
ClassAttrIndex gates enum harvesting on "Enum" appearing in a base expr and on
EVERY member carrying a string-literal value -- both hold here."""

from enum import StrEnum


class DocTopicName(StrEnum):
    REVIEW = "kyc.doc.review"
    AUDIT = "kyc.doc.audit"
    ARCHIVE = "kyc.doc.archive"
