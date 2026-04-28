/*
 * picodata.io top header — drawer + accordion behaviour for the mobile
 * view. Mirrors the inline script in Header.astro on the marketing site
 * (picodata-landing repo).
 *
 * Desktop dropdowns are pure-CSS (:hover on .menu-item shows the
 * .mega-panel) — no JS needed.
 */

;(function () {
  "use strict";

  function init() {
    var hamburger = document.querySelector(".picodata-header .hamburger");
    var drawer = document.getElementById("site-drawer");
    var closeBtn = document.querySelector(".picodata-header .drawer-close");
    if (!hamburger || !drawer) return;

    function openDrawer() {
      document.body.classList.add("picodata-menu-open");
      drawer.setAttribute("aria-hidden", "false");
      hamburger.setAttribute("aria-expanded", "true");
    }

    function closeDrawer() {
      document.body.classList.remove("picodata-menu-open");
      drawer.setAttribute("aria-hidden", "true");
      hamburger.setAttribute("aria-expanded", "false");
    }

    hamburger.addEventListener("click", openDrawer);
    if (closeBtn) closeBtn.addEventListener("click", closeDrawer);

    document.addEventListener("keydown", function (e) {
      if (e.key === "Escape" && document.body.classList.contains("picodata-menu-open")) {
        closeDrawer();
      }
    });

    // Accordion-style sections inside the drawer.
    drawer.querySelectorAll(".drawer-trigger").forEach(function (trig) {
      trig.addEventListener("click", function () {
        var section = trig.closest(".drawer-section");
        if (!section) return;
        var willOpen = !section.classList.contains("is-open");
        section.classList.toggle("is-open", willOpen);
        trig.setAttribute("aria-expanded", willOpen ? "true" : "false");
      });
    });

    // Close drawer when a navigation link is clicked. Same-document
    // anchor jumps don't unmount the drawer, so without this clicks
    // like /services/#smart_start would appear to do nothing.
    drawer.addEventListener("click", function (e) {
      var link = e.target.closest("a[href]");
      if (link && drawer.contains(link)) closeDrawer();
    });
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", init);
  } else {
    init();
  }
})();
