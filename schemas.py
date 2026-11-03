"""
Shared Pydantic schemas for the Local Regulatory Change Radar (LRCR).

These live in their own module because both the Tier 1 parser
(`pdf_parser.py`) and the API (`main.py`) need them, and importing them from
`main.py` would create a circular import.

`GazetteAnalysisResult` is also used as the structured-output schema for
the LLM, so keep it JSON-schema friendly: no custom types, no validators
that reject plausible LLM output, descriptions on every field (the model
reads them).
"""

from __future__ import annotations

from typing import Optional

from pydantic import BaseModel, ConfigDict, Field, field_validator


class MissingDocumentRef(BaseModel):
    """A document the gazette references but does not contain."""

    model_config = ConfigDict(str_strip_whitespace=True)

    institution: str = Field(
        ..., min_length=2,
        description="Name of the public enterprise that issued or holds the document, "
                    "exactly as written in the gazette.",
    )
    decision_number: str = Field(
        ..., min_length=1,
        description="The document's reference number, e.g. '02-164/7' or '08-1234/1'. "
                    "Only the number, without 'бр.' or 'број'.",
    )
    expected_content: str = Field(
        default="",
        description="One sentence, in Macedonian, describing what the document is "
                    "(its title or subject as the gazette describes it).",
    )

    @field_validator("decision_number")
    @classmethod
    def _sane_decision_number(cls, v: str) -> str:
        if len(v) > 64:
            raise ValueError("decision_number is implausibly long")
        return v


class RegulatorySignal(BaseModel):
    """One regulatory change detected in a gazette."""

    model_config = ConfigDict(str_strip_whitespace=True)

    title: Optional[str] = Field(
        default=None,
        description="Short title of the council decision or act, in Macedonian.",
    )
    summary: Optional[str] = Field(
        default=None,
        description="One or two sentences on what changes for businesses and residents.",
    )
    is_actionable_immediately: bool = Field(
        ..., description="True when the gazette text itself contains everything needed to "
                         "understand and act on the change (prices, dates, obligations). "
                         "False when it only references or approves another document "
                         "whose content is not reproduced.",
    )
    missing_references: list[MissingDocumentRef] = Field(
        default_factory=list,
        description="Documents referenced but not reproduced in the gazette. Empty when "
                    "is_actionable_immediately is true.",
    )


class GazetteAnalysisResult(BaseModel):
    """Tier 1 output for one gazette issue."""

    model_config = ConfigDict(str_strip_whitespace=True)

    gazette_id: str = Field(
        ..., min_length=1,
        description="Identifier of the gazette issue. Use the value you are given.",
    )
    published_on: Optional[str] = Field(
        default=None,
        description="Publication date in ISO format (YYYY-MM-DD) if it appears in the text.",
    )
    signals: list[RegulatorySignal] = Field(
        default_factory=list,
        description="Every business-impacting decision found in the gazette.",
    )


__all__ = ["MissingDocumentRef", "RegulatorySignal", "GazetteAnalysisResult"]
