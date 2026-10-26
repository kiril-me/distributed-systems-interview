# Constraints for this repo
- Fixed: services/driver/instrument/instrument.go, the Executor method set,
  Serve's signature, RunView/StepView, status values, data_ref as
  s3://bucket/key, HTTP paths. acceptance.sh must pass with faults at 0.
- Do not write code until a design decision is agreed.
- Never overwrite a file you have not read. Never run docker compose down -v
  unless asked.
- Every claim about behaviour needs evidence: a log line, a ledger entry or a
  query. Say "untested" otherwise.
- Prefer the smallest change. Flag scope creep instead of doing it.
