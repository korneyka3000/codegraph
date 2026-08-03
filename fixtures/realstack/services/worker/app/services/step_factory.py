"""M11 T1 realstack leg (task-1, rerun-5 open-gaps R6, docs/superpowers/reports/
2026-08-03-pilot-rerun-5-open-gaps.md): a classmethod-factory `return cls(...)`
self-construction call carries scip-python's trailing Parameter-descriptor for
the "cls" parameter itself (`...#from_label().(cls)`, confirmed against REAL
scip-python 0.6.6 -- task-1-report.md's own Step 1 occurrence dump).
READ-FIRST FINDING: this is NOT, as the pilot's own R6 diagnosis first framed
it, a disambiguator on an EXTERNAL `Cls.make(...)` call -- ten such external/
instance/subclass/sibling-dispatch call shapes were tried against real
scip-python and NONE carry a tail (`build_step_details` below resolves cleanly
today, already, pre-fix). The ONLY shape that carries the tail is a
bare-identifier call whose callee expression IS ITSELF the "cls" parameter --
i.e. the classmethod's own internal `return cls(...)` construction, exactly
`StepDetails.from_label`'s own body below.

`extractors/calls.py`'s `_cls_construction_dst` (see that module's own M11 T1
section for the guards) resolves the VERIFIED idiom to the ENCLOSING CLASS
node -- `cls` inside a @classmethod is the language-guaranteed
`type[EnclosingClass]`, so `cls(label)` IS construction of `StepDetails`,
exactly like a direct `StepDetails(label)` ctor call's own ref (`StepDetails#`)
already resolves today (task-1 review, construction semantics) -- the
recovered CALLS edge is `from_label -> StepDetails` (method constructs its
class), NOT a self-loop and NOT a dangling parameter symbol. See
tests/eval/test_m10_gate.py's own M11 T1 pin for the live, real-scip proof.

Deliberately a brand-new, otherwise-unreferenced file/class (mirrors app/
routes/admin.py's own M9 T3/M10 T1 isolation reasoning) so this leg disturbs
no existing golden/trace pin."""


class StepDetails:
    """Fixture stand-in for the pilot's own dropped-CALLS shape -- pydantic-
    style factory models in the real corpus (`CreateStepDetails#
    from_decision().(cls)`, `GetVerificationRequestActivityInput#
    with_all_steps().(cls)`, `CreateCaseInput#build_sdf_sof().(cls)`): a
    classmethod factory whose body does `return cls(...)`."""

    def __init__(self, label: str) -> None:
        self.label = label

    @classmethod
    def from_label(cls, label: str) -> "StepDetails":
        return cls(label)


def build_step_details(label: str) -> "StepDetails":
    """External caller (brief's own "factory + a call to Cls.make() from
    ANOTHER method" leg) -- `StepDetails.from_label(label)` resolves CLEANLY
    even pre-fix (Step 1 confirmed an EXTERNAL ClassName.method() call never
    carries a parameter tail; only the classmethod's OWN internal `cls(...)`
    does, see the module docstring above). Included for parity with the real
    corpus's own usage shape (a factory called from activity/workflow code
    elsewhere) -- not itself the edge under test."""
    return StepDetails.from_label(label)
