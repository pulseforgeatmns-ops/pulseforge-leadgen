'use strict';

/**
 * Mail Package Generator capability (SPEC-033).
 * Print-ready direct mail packages for approved campaign prospects.
 */

const {
  CAPABILITY_CATEGORIES,
  BUILTIN_IDS,
  buildCapabilityResult,
  buildCapabilityEstimate,
  CAPABILITY_RESULT_STATUS,
  PROGRESS_KINDS,
} = require('../types');
const {
  MAIL_PROGRESS_STAGES,
  OPERATOR_ACTIONS,
  PACKAGE_STATUS,
  DEFAULT_CONFIDENCE_THRESHOLD,
  DEFAULT_INSERT_CHECKLIST,
  buildMailPackage,
  buildCampaignSummary,
} = require('./types');
const { summarizePackageStatuses } = require('./validate');
const { composeMailPackage } = require('./personalize');
const { renderMailPackageHtml, renderCampaignHtml } = require('./render');
const { buildMailMergeCsv, buildAddressLabelCsv } = require('./exportCsv');
const { buildPackageDocx, buildCampaignDocx } = require('./exportDocx');
const { createInMemoryMailPackageStore } = require('./MailPackageStore');
const {
  resolvePlaybookFromContext,
  campaignStrategyFromPlaybook,
} = require('../playbook');

/**
 * Resolve approved prospect list from campaign / inputs.
 * @param {object} context
 * @returns {{ prospects: object[], campaign: object|null, mailMerge: object[] }}
 */
function resolveApprovedProspects(context = {}) {
  const inputs = context.inputs || {};
  const prior = inputs.priorOutputs || {};
  const constraints = context.constraints || {};

  const campaign =
    inputs.campaign ||
    prior.campaign ||
    constraints.campaign ||
    null;

  let prospects =
    inputs.approvedProspects ||
    inputs.prospects ||
    (campaign && campaign.prospects) ||
    prior.prospects ||
    [];

  if (!Array.isArray(prospects)) prospects = [];

  // Prefer explicitly approved subset when provided
  if (Array.isArray(inputs.approvedProspectIds) && inputs.approvedProspectIds.length) {
    const allow = new Set(inputs.approvedProspectIds.map(String));
    prospects = prospects.filter((p) => allow.has(String(p.id)));
  }

  const mailMerge =
    (campaign && Array.isArray(campaign.mailMerge) && campaign.mailMerge) ||
    (Array.isArray(inputs.mailMerge) && inputs.mailMerge) ||
    [];

  return { prospects, campaign, mailMerge };
}

/**
 * @param {object} prospect
 * @param {object[]} mailMerge
 * @returns {object|null}
 */
function matchMailMergeRow(prospect, mailMerge) {
  if (!Array.isArray(mailMerge) || !mailMerge.length) return null;
  const name = String(prospect.companyName || '').toLowerCase();
  return (
    mailMerge.find(
      (row) =>
        String(row.companyName || '').toLowerCase() === name ||
        (prospect.id != null && String(row.prospectId) === String(prospect.id))
    ) || null
  );
}

/**
 * @param {object} [deps]
 */
function createMailPackageGeneratorCapability(deps = {}) {
  const store = deps.mailPackageStore || createInMemoryMailPackageStore();

  return {
    id: BUILTIN_IDS.MAIL_PACKAGE_GENERATOR,
    name: 'Generating Mail Packages',
    description:
      'Produce print-ready direct mail packages for approved campaign prospects',
    category: CAPABILITY_CATEGORIES.CAMPAIGN,
    outcomeTags: ['mail_packages_generated', 'mail_ready_for_review'],
    retryable: true,
    timeoutMs: 90_000,
    supportsRollback: false,
    idempotent: false,
    inputSchema: {
      required: ['prospects'],
      properties: {
        prospects: 'Prospect[]',
        approvedProspects: 'Prospect[]?',
        campaign: 'Campaign?',
        clientPlaybook: 'ClientPlaybook?',
        companyIntelligence: 'object?',
        confidenceThreshold: 'number?',
        returnAddress: 'string?',
        insertChecklist: 'InsertItem[]?',
      },
    },
    outputSchema: {
      mailBatch: 'MailBatchRevision',
      packages: 'MailPackage[]',
      campaignSummary: 'CampaignSummary',
      campaignHtml: 'string',
      mailMergeCsv: 'string',
      addressLabelCsv: 'string',
      reviewPackage: 'object',
    },

    canRun(context) {
      const { prospects } = resolveApprovedProspects(context || {});
      return prospects.length > 0;
    },

    estimate(context) {
      const { prospects } = resolveApprovedProspects(context || {});
      const n = prospects.length || 1;
      return buildCapabilityEstimate({
        durationMs: 1200 + n * 180,
        confidence: prospects.length ? 0.88 : 0.35,
        notes: prospects.length
          ? [`Mail packages for ${prospects.length} approved prospect(s)`]
          : ['No approved prospects provided'],
      });
    },

    /**
     * @param {object} context
     * @param {{ onProgress?: Function }} [runtime]
     */
    async execute(context, runtime = {}) {
      const started = Date.now();
      const emit = (stage, pct) => {
        if (typeof runtime.onProgress === 'function') {
          runtime.onProgress({
            kind: PROGRESS_KINDS.PROGRESS,
            stage,
            percent: pct,
            message: stage,
          });
        }
      };

      emit(MAIL_PROGRESS_STAGES.GATHERING, 8);

      const { prospects, campaign, mailMerge } = resolveApprovedProspects(context);
      if (!prospects.length) {
        return buildCapabilityResult({
          status: CAPABILITY_RESULT_STATUS.FAILED,
          errors: [
            {
              code: 'missing_approved_prospects',
              message:
                'Mail Package Generator requires an approved prospect list (campaign.prospects or inputs.prospects)',
            },
          ],
          duration: Date.now() - started,
        });
      }

      const playbook = resolvePlaybookFromContext(context);
      const strategy =
        (campaign && campaign.playbook) ||
        campaignStrategyFromPlaybook(playbook) ||
        null;

      const inputs = context.inputs || {};
      const prior = inputs.priorOutputs || {};
      const salesIntelligenceByProspectId =
        inputs.salesIntelligenceByProspectId ||
        prior.salesIntelligenceByProspectId ||
        {};
      const salesIntelligenceProfiles =
        inputs.salesIntelligenceProfiles ||
        prior.salesIntelligenceProfiles ||
        prior.profiles ||
        [];
      const businessIntelligenceByProspectId =
        inputs.businessIntelligenceByProspectId ||
        prior.businessIntelligenceByProspectId ||
        {};
      const businessIntelligenceProfiles =
        inputs.businessIntelligenceProfiles ||
        prior.businessIntelligenceProfiles ||
        [];
      const confidenceThreshold =
        Number.isFinite(Number(inputs.confidenceThreshold))
          ? Number(inputs.confidenceThreshold)
          : Number.isFinite(
                Number(context.constraints && context.constraints.confidenceThreshold)
              )
            ? Number(context.constraints.confidenceThreshold)
            : DEFAULT_CONFIDENCE_THRESHOLD;

      const overrides = inputs.packageOverrides || {};
      const operatorEdits = inputs.letterEdits || {};

      emit(MAIL_PROGRESS_STAGES.VALIDATING, 20);
      emit(MAIL_PROGRESS_STAGES.COMPOSING, 40);

      const packages = prospects.map((prospect, index) => {
        const key = String(prospect.id != null ? prospect.id : index);
        const ov = overrides[key] || overrides[prospect.companyName] || {};
        const mailMergeRow = matchMailMergeRow(prospect, mailMerge);
        const composed = composeMailPackage(prospect, {
          playbook,
          campaignStrategy: strategy,
          mailMergeRow,
          opportunityBrief:
            prospect.opportunityBrief ||
            (inputs.opportunityBriefs && inputs.opportunityBriefs[key]) ||
            null,
          companyIntelligence:
            prospect.companyIntelligence ||
            (inputs.companyIntelligencePackages &&
              inputs.companyIntelligencePackages[key]) ||
            inputs.companyIntelligence ||
            null,
          businessIntelligence:
            prospect.businessIntelligenceProfile ||
            prospect.businessIntelligence ||
            businessIntelligenceByProspectId[key] ||
            null,
          businessIntelligenceByProspectId,
          businessIntelligenceProfiles,
          salesIntelligenceProfile:
            prospect.salesIntelligenceProfile ||
            (mailMergeRow && mailMergeRow.salesIntelligence) ||
            salesIntelligenceByProspectId[key] ||
            null,
          salesIntelligenceByProspectId,
          salesIntelligenceProfiles,
          confidenceThreshold,
          returnAddress: inputs.returnAddress || ov.returnAddress,
          insertChecklist: inputs.insertChecklist || ov.insertChecklist,
          signature: inputs.signature || ov.signature,
          clientName: inputs.clientName,
          skipped: ov.skipped === true,
          addressInvalid: ov.addressInvalid === true,
          recipientOverride:
            ov.recipientName ||
            (operatorEdits[key] && operatorEdits[key].recipientName) ||
            null,
        });

        // Apply letter body edits when operator provided them
        const edit = operatorEdits[key] || ov.letter || null;
        if (edit && typeof edit === 'object') {
          composed.letter = {
            ...composed.letter,
            ...edit,
            body:
              edit.body ||
              composed.letter.body,
          };
        }

        const pkg = buildMailPackage({
          id: `pkg_${key}_r`,
          ...composed,
          revision: 1,
        });

        // Recompute status if approved flag set and still valid
        if (ov.approve === true && pkg.status === PACKAGE_STATUS.READY_TO_PRINT) {
          pkg.status = PACKAGE_STATUS.APPROVED;
          pkg.approved = true;
        }

        const rendered = renderMailPackageHtml(pkg);
        const docx = buildPackageDocx(pkg);
        pkg.html = rendered.packageHtml;
        pkg.letterHtml = rendered.letterHtml;
        pkg.envelopeHtml = rendered.envelopeHtml;
        pkg.summaryHtml = rendered.summaryHtml;
        pkg.docxHtml = docx.docxHtml;
        pkg.docxFilename = docx.filename;
        return pkg;
      });

      emit(MAIL_PROGRESS_STAGES.RENDERING, 70);

      const statusCounts = summarizePackageStatuses(packages);
      const insertCount =
        (packages[0] && packages[0].insertChecklist && packages[0].insertChecklist.length) ||
        DEFAULT_INSERT_CHECKLIST.length;
      const campaignSummary = buildCampaignSummary({
        ...statusCounts,
        insertCount,
      });

      const campaignName =
        (campaign && campaign.name) ||
        inputs.campaignName ||
        'Campaign Mail Packages';

      const generatedAt = new Date().toISOString();
      const campaignHtml = renderCampaignHtml(packages, {
        campaignName,
        campaignSummary,
        revision: 1,
        generatedAt,
      });

      emit(MAIL_PROGRESS_STAGES.EXPORTING, 85);

      const mailMergeExport = buildMailMergeCsv(packages);
      const addressLabelExport = buildAddressLabelCsv(packages);
      const campaignDocx = buildCampaignDocx(packages, { campaignName });

      const campaignKey =
        inputs.campaignId ||
        (campaign && (campaign.id || campaign.name)) ||
        context.missionId ||
        null;

      const saved = await Promise.resolve(
        store.create({
          campaignId: campaignKey,
          missionId: context.missionId || null,
          clientId: context.clientId != null ? context.clientId : null,
          tenantId: context.tenantId || '',
          status: 'review',
          campaignName,
          packages,
          campaignSummary,
          campaignHtml,
          mailMergeCsv: mailMergeExport.csv,
          addressLabelCsv: addressLabelExport.csv,
          campaignDocxHtml: campaignDocx.docxHtml,
          playbookId: playbook ? playbook.id : null,
          playbookVersion: playbook ? playbook.version : null,
          confidenceThreshold,
        })
      );

      // Stamp revision onto packages for operator view
      for (const pkg of packages) {
        pkg.revision = saved.revision;
        pkg.batchId = saved.id;
      }

      emit(MAIL_PROGRESS_STAGES.COMPLETED, 100);

      const reviewPackage = {
        summary: `${campaignName}: ${campaignSummary.readyToPrint} ready to print, ${campaignSummary.needsReview} need review`,
        operatorActions: [...OPERATOR_ACTIONS],
        campaignSummary,
        confidenceThreshold,
        exports: ['pdf', 'docx', 'csv'],
        warnings: packages.flatMap((p) => p.warnings || []).slice(0, 20),
      };

      const readyBlocked = campaignSummary.needsReview > 0;

      return buildCapabilityResult({
        status: CAPABILITY_RESULT_STATUS.COMPLETED,
        outputs: {
          mailBatch: saved,
          mailBatchId: saved.id,
          mailBatchRevision: saved.revision,
          packages,
          campaignSummary,
          campaignHtml,
          printableHtml: campaignHtml,
          mailMergeCsv: mailMergeExport.csv,
          addressLabelCsv: addressLabelExport.csv,
          campaignDocxHtml: campaignDocx.docxHtml,
          exports: {
            pdf: { format: 'printable_html', html: campaignHtml },
            docx: {
              format: campaignDocx.format,
              html: campaignDocx.docxHtml,
              filename: campaignDocx.filename,
            },
            csv: {
              mailMerge: mailMergeExport.filename,
              addressLabels: addressLabelExport.filename,
            },
          },
          reviewPackage,
          clientPlaybook: playbook,
          clientPlaybookId: playbook ? playbook.id : null,
          clientPlaybookVersion: playbook ? playbook.version : null,
          printBlocked: readyBlocked,
          confidence: campaignSummary.prospects
            ? campaignSummary.readyToPrint / campaignSummary.prospects
            : 0,
        },
        evidence: [
          {
            kind: 'mail_package',
            summary: playbook
              ? `Generated ${packages.length} mail package(s) for ${campaignName} using playbook ${playbook.name} v${playbook.version}`
              : `Generated ${packages.length} mail package(s) for ${campaignName}`,
            readyToPrint: campaignSummary.readyToPrint,
            needsReview: campaignSummary.needsReview,
            missingAddresses: campaignSummary.missingAddresses,
            revision: saved.revision,
            playbookId: playbook ? playbook.id : null,
            playbookVersion: playbook ? playbook.version : null,
          },
        ],
        artifacts: [
          {
            type: 'mail_package_batch',
            id: saved.id,
            revision: saved.revision,
            campaignName,
          },
          {
            type: 'campaign_pdf',
            id: saved.id,
            format: 'printable_html',
          },
          {
            type: 'mail_merge_csv',
            id: saved.id,
            filename: mailMergeExport.filename,
          },
          {
            type: 'address_label_csv',
            id: saved.id,
            filename: addressLabelExport.filename,
          },
          {
            type: 'campaign_docx',
            id: saved.id,
            format: campaignDocx.format,
            filename: campaignDocx.filename,
          },
        ],
        warnings: readyBlocked
          ? [
              'Some packages need review — Ready-to-Print export should exclude invalid rows until fixed',
              ...reviewPackage.warnings.slice(0, 5),
            ]
          : [],
        nextRecommendations: [
          {
            action: 'review',
            summary:
              'Review Needs Review packages before printing; Ready-to-Print only after validation passes',
          },
          {
            action: 'export_csv',
            summary: 'Download mail merge and address label CSVs for the print vendor',
          },
        ],
        duration: Date.now() - started,
      });
    },
  };
}

module.exports = {
  createMailPackageGeneratorCapability,
  resolveApprovedProspects,
  matchMailMergeRow,
};
