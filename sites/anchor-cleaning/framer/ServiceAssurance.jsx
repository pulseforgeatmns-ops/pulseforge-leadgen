/**
 * Anchor Service Assurance portal section — Framer-ready building blocks.
 * Swap `imageSrc` props when production portal screenshots are available.
 */

export function PortalMockupFrame({
  imageSrc,
  alt,
  caption,
  variant = "default",
  className = "",
}) {
  const variantClass =
    variant === "primary"
      ? "portal-mockup-frame--primary"
      : variant === "device"
        ? "portal-mockup-frame--device"
        : "";
  return (
    <figure className={`portal-mockup-frame ${variantClass} ${className}`.trim()}>
      <img src={imageSrc} alt={alt} loading="lazy" decoding="async" />
      {caption ? <figcaption>{caption}</figcaption> : null}
    </figure>
  );
}

export function ServiceAssuranceProofPoints({ items }) {
  return (
    <ul className="service-assurance-proof">
      {items.map((item) => (
        <li key={item.title}>
          <h3>{item.title}</h3>
          <p>{item.body}</p>
        </li>
      ))}
    </ul>
  );
}

export function ServiceAssuranceHero({
  eyebrow = "Service Assurance",
  headline = "How Anchor solves inconsistency",
  body,
  proofItems,
  primaryCtaHref = "#contact",
  primaryCtaLabel = "Book a Facility Assessment",
  dashboardImageSrc,
  dashboardAlt,
  dashboardCaption,
}) {
  return (
    <div className="service-assurance-hero doc">
      <div className="service-assurance-story">
        <p className="service-assurance-eyebrow rv">{eyebrow}</p>
        <h2 className="rv">{headline}</h2>
        <div className="service-assurance-body rv">{body}</div>
        <ServiceAssuranceProofPoints items={proofItems} />
        <a className="btn-primary service-assurance-cta rv" href={primaryCtaHref}>
          {primaryCtaLabel}
        </a>
      </div>
      <PortalMockupFrame
        variant="primary"
        imageSrc={dashboardImageSrc}
        alt={dashboardAlt}
        caption={dashboardCaption}
        className="rv"
      />
    </div>
  );
}

export function PortalFeaturePanel({
  title,
  body,
  imageSrc,
  alt,
  tone = "light",
  imageVariant = "default",
}) {
  const toneClass = tone === "cream" ? "portal-feature-panel--cream" : "";
  return (
    <article className={`portal-feature-panel ${toneClass} rv`}>
      <PortalMockupFrame
        variant={imageVariant}
        imageSrc={imageSrc}
        alt={alt}
      />
      <h3>{title}</h3>
      <p>{body}</p>
    </article>
  );
}

export function FacilityAssessmentCTA({
  headline = "Let’s define the standard for your facility.",
  body,
  buttonHref = "#contact",
  buttonLabel = "Book a Facility Assessment",
}) {
  return (
    <div className="facility-assessment-cta">
      <div className="facility-assessment-cta-inner doc rv">
        <h2>{headline}</h2>
        <p>{body}</p>
        <a className="btn-primary" href={buttonHref}>
          {buttonLabel}
        </a>
      </div>
    </div>
  );
}

const DEFAULT_PROOF = [
  {
    title: "Defined service scope",
    body: "Every location has a clear, documented cleaning scope so expectations do not live in someone's memory.",
  },
  {
    title: "Cleaner accountability",
    body: "Cleaners work from visit-specific checklists and can flag exceptions, notes, and issues as they happen.",
  },
  {
    title: "Visible issue resolution",
    body: "Clients can report concerns, see when they've been acknowledged, and follow the issue through resolution.",
  },
];

const DEFAULT_BODY = (
  <>
    <p>
      The Anchor Standard is backed by a system. A clean facility should not depend on who happened to show up that night.
    </p>
    <p>
      Anchor&apos;s service system gives every location a defined scope, gives cleaners a clear way to document completion and exceptions, and gives clients visibility when something needs attention.
    </p>
    <p>
      The result is a cleaning program that is easier to manage, easier to correct, and far more accountable.
    </p>
  </>
);

const DEFAULT_FACILITY_BODY =
  "We'll walk your facility, understand where your current service is falling short, and build a scope around the standards that matter to your team.";

export function ServiceAssuranceSection({
  dashboardImageSrc = "/assets/service-assurance/client-dashboard.png",
  cleanerImageSrc = "/assets/service-assurance/cleaner-checklist.png",
  issuesImageSrc = "/assets/service-assurance/issues-resolution.png",
}) {
  return (
    <section className="service-assurance" id="service-assurance" aria-labelledby="service-assurance-heading">
      <ServiceAssuranceHero
        body={DEFAULT_BODY}
        proofItems={DEFAULT_PROOF}
        dashboardImageSrc={dashboardImageSrc}
        dashboardAlt="Anchor Service Assurance Portal showing recent cleaning visits, service status, next service, open issues, and facility scope."
        dashboardCaption="See completed visits, upcoming service, open issues, and the defined scope for your facility in one place."
      />
      <div className="service-assurance-panels doc" id="service-assurance-how">
        <PortalFeaturePanel
          tone="light"
          imageVariant="device"
          imageSrc={cleanerImageSrc}
          alt="Anchor cleaner mobile checklist showing completed tasks, flagged issues, and visit exception documentation."
          title="A clear scope for every visit"
          body="Cleaners work from location-specific checklists, document completion, and flag anything that needs attention before the visit is closed."
        />
        <PortalFeaturePanel
          tone="cream"
          imageSrc={issuesImageSrc}
          alt="Anchor client portal issue-resolution screen showing a missed kitchen sink report progressing from open to resolved."
          title="When something is missed, it gets handled"
          body="Clients can report an issue directly, see when Anchor acknowledges it, and follow the resolution through completion."
        />
      </div>
      <FacilityAssessmentCTA body={DEFAULT_FACILITY_BODY} />
    </section>
  );
}

export default ServiceAssuranceSection;
