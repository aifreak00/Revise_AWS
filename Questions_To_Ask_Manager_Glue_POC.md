# 💬 Smart Questions to Ask Your Manager
## About the AWS Glue POC Solution Design Document
### These show you READ it deeply + think like a senior engineer

---

> 🎯 **Strategy:** Don't ask what's already answered in the doc.
> Ask about things that are INTENTIONALLY left open, ambiguous,
> or could go multiple ways. That's what impresses leads.

---

## 🔴 PRIORITY 1 — Ask These First (Most Impressive)

---

**Q1. On the delta load — the doc mentions watermark-based approach
but says "the approach will be determined during the POC."
Should we start with a fixed 2-hour overlap window,
or do you want us to test both with and without overlap
and compare which one captures late-arriving records better?**

> WHY THIS IS GOOD:
> You read Section 6.5 carefully. It says "approach to be determined."
> You're not just asking what to do — you're proposing two options.
> Shows you understand late-arriving data is a real problem in production.

---

**Q2. The doc says we compare Glue data against existing SnapLogic data
in Snowflake using MINUS queries. But since both pipelines run at
different times, won't there be a natural time gap where SnapLogic
might have more records just because it ran earlier?
How do we want to handle that timing window during validation —
should we freeze the SnapLogic data at a specific timestamp
before we start the Glue run?**

> WHY THIS IS GOOD:
> This is a REAL problem nobody mentioned in the doc.
> Your lead will either say "great catch" or explain how they planned to handle it.
> Either way you look sharp.

---

**Q3. The SAP firewall whitelisting is marked "In Progress, waiting on infra."
If that's not resolved by Feb 26 — end of Week 1 —
should we pivot to doing SFDC first for the 2-table extraction
in Week 2, and keep SAP as Week 2 parallel work?
Or is there a hard dependency we should wait for?**

> WHY THIS IS GOOD:
> You looked at the Excel plan, not just the design doc.
> You spotted the actual blocker that could break the entire Week 1 timeline.
> You're thinking about risk mitigation, not just execution.

---

**Q4. The doc says we use "existing Glue transformation templates" for the
Snowflake load step — no changes needed.
Just to confirm my understanding: those existing templates currently
read from SnapLogic's S3 paths. For the POC, will we just update
the source S3 path parameter to point to our new Glue extraction output,
or is there a different approach planned?**

> WHY THIS IS GOOD:
> Section 6.6 says "use existing templates — no change."
> But the templates must be reading from a specific S3 path.
> Asking this shows you thought through the actual implementation detail.
> Your lead either confirms it or clarifies something important.

---

**Q5. For the 2 tables we're doing in Week 2 —
has a decision already been made on WHICH 2 tables,
or should I recommend based on what would give us
the most meaningful POC coverage?
I was thinking one high-volume table like SecondarySalesLineItem
and one simpler one like Account — to test performance
at both ends of the spectrum. Does that work?**

> WHY THIS IS GOOD:
> The Excel plan says "Table 1" and "Table 2" — no names specified.
> You read that gap. You're not waiting to be told — you're proposing.
> And your proposal is logical (high volume + low volume = good test).

---

## 🟡 PRIORITY 2 — Ask These if the Conversation is Going Well

---

**Q6. For the ACDOCA_FINANCE_DATA table — 418 million rows, 28 GB —
the OData $skip/$top pagination approach could be very slow
because SAP recalculates the full result set on every page.
Has the SAP team confirmed this OData service supports
server-side cursors or next-link pagination,
or are we expecting to use pure skip/top?
I want to plan DPU sizing accordingly.**

> WHY THIS IS GOOD:
> $skip/$top on 418M rows = very slow (each page re-scans the table).
> OData v4 has @odata.nextLink which is faster.
> Asking this shows you know SAP OData internals.

---

**Q7. The control table is being created in Snowflake,
but the current SnapLogic config lives in MySQL RDS.
Should our GLUE_ENTITY_CONFIG table structure
stay aligned with the existing INPUT_ENTITY_LIST columns,
or is this a chance to clean up / redesign the schema?
I don't want to create something that's hard to maintain
when we migrate all remaining tables later.**

> WHY THIS IS GOOD:
> You read Section 7 (control table design) AND Section 4.1 (current state MySQL RDS).
> You're thinking beyond the POC to full production migration.
> Lead will appreciate forward-thinking.

---

**Q8. For error handling — if the Glue job fails midway
through extracting a 100M+ row table,
should we restart from the beginning (full re-extract)
or do we want a checkpoint mechanism to resume from where it failed?
The doc doesn't cover partial failure recovery.
Is that out of scope for POC, or should we think about it now?**

> WHY THIS IS GOOD:
> Document is silent on this. It's a real production concern.
> A 418M row table failing at page 3000 out of 5000 = 2 hours wasted.
> Asking now saves pain later.

---

**Q9. The success criteria says "Performance: comparable to SnapLogic."
Do we have actual SnapLogic job run times documented
for these specific tables?
I want to make sure we're comparing against real numbers,
not just doing Glue in isolation and declaring it acceptable.**

> WHY THIS IS GOOD:
> "Comparable to SnapLogic" is vague without a baseline.
> You can't measure success without a benchmark.
> Smart performance engineering question.

---

**Q10. The doc mentions loading to "GLUE_POC_DB.{SOURCE}_CORE"
as a separate schema for comparison.
Who has access to create schemas and tables in Snowflake?
Do I need to raise a request, or do I already have DDL permissions
in the POC environment?**

> WHY THIS IS GOOD:
> Purely practical. Shows you're thinking about what you need to START.
> Access provisioning takes time. Better to ask Day 1.

---

## 🟢 PRIORITY 3 — Quick Practical Clarifiers (Ask Casually)

---

**Q11. "For the SAP OData APIs — are these CDS view-based APIs
that already exist, or do we need the SAP Basis team
to create new OData services for these entities?
The document assumes APIs exist but I wanted to confirm."**

> If APIs don't exist yet → that's a Week 1 blocker nobody flagged.

---

**Q12. "Should I use Glue 4.0 (Spark 3.3) or an older version?
And for the Python shell vs PySpark choice —
for the connectivity test jobs, is a Python shell job fine
or should everything be PySpark from the start?"**

> Shows technical awareness. Python shell = simpler + cheaper for light jobs.

---

**Q13. "Is there a Glue development endpoint or local testing setup,
or should I test directly by running Glue jobs in AWS?
I want to avoid unnecessary Glue job costs during development."**

> Practical cost-awareness question. Leads like engineers who think about cost.

---

**Q14. "For the Snowflake POC schema — should I log the audit entries
in the same PRD_ARCH_SUPPORT_DB database,
or is there a separate DEV/POC database I should be using
to keep production clean?"**

> Shows you care about not polluting prod. Very professional.

---

## 💡 HOW TO ASK THESE — The Right Way

```
OPENING LINE (say this first):
────────────────────────────────
"I went through both the solution design document and the
 March 15 plan carefully. I have a good overall understanding
 of what we're building. I had a few questions on some
 areas that seem intentionally open — wanted to align
 before I start coding."

Then pick 3-4 questions from Priority 1 above.
Don't ask all 14 at once. That feels like you're dumping homework.

AFTER YOUR LEAD ANSWERS:
──────────────────────────
Follow up with:
"That makes sense. One more thing — [next question]"

This feels like a natural conversation, not an interview.

END WITH:
──────────────────────────
"I was thinking I'll start with the SFDC connectivity test
 first since SAP firewall is still pending.
 Does that approach work for you?"

This shows you're not waiting to be told what to do.
You have a plan. You just want alignment.
```

---

## ⭐ THE TOP 3 IF YOU ONLY ASK 3

```
If your manager seems busy and you can only ask 3, pick these:

  1. Q3 — SAP firewall blocker + SFDC pivot plan
     (shows you read the Excel plan AND thought about risk)

  2. Q2 — timing gap problem in validation
     (shows you spotted a real bug in the approach)

  3. Q5 — which 2 tables to start with + your recommendation
     (shows initiative — you're not waiting to be assigned)

These 3 together will make your manager think:
"This person read the document properly and is thinking ahead."
That's exactly the impression you want on Day 1.
```

---

*Smart questions > saying "I understood everything."*
*Your lead knows nobody understands everything on Day 1.*
*Good questions show you read it. Great questions show you THOUGHT about it.*
