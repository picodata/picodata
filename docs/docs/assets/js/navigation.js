;(function () {
  "use strict"

  /* =========================
   *  Общие утилиты
   * ========================= */

  function qs(selector, scope) {
    return (scope || document).querySelector(selector)
  }

  function qsa(selector, scope) {
    return (scope || document).querySelectorAll(selector)
  }

  function on(el, event, handler, options) {
    if (!el) return
    el.addEventListener(event, handler, options || false)
  }

  /* =========================
   *  Навигация (header menu)
   * ========================= */

  function getPaddingLeftPx(el) {
    var style = window.getComputedStyle(el)
    var pl = parseFloat(style.paddingLeft || "0")
    return Number.isFinite(pl) ? pl : 0
  }

  function updateSubmenuGap() {
    var menu = qs(".header__menu")
    var submenus = qsa(".header-menu__submenu-block")
    if (!menu || !submenus.length) return

    var menuLeft = menu.getBoundingClientRect().left

    submenus.forEach(function (submenu) {
      var headline = qs(".submenu__headline", submenu)
      if (!headline) return

      var submenuPaddingLeft = getPaddingLeftPx(submenu)
      var headlineWidth = headline.getBoundingClientRect().width
      var gap = menuLeft - (submenuPaddingLeft + headlineWidth)

      if (!Number.isFinite(gap) || gap < 0) gap = 0

      if (submenu.classList.contains("header-menu__submenu-block--open")) {
        submenu.style.gap = gap + "px"
      }
    })
  }

  function initHeaderMenuHover() {
    var items = qsa(".header-menu__menu-item--dropdown")
    if (!items.length) return

    items.forEach(function (item) {
      var submenu = qs(".header-menu__submenu-block", item)
      if (!submenu) return

      var hideTimeout = null

      var show = function () {
        clearTimeout(hideTimeout)
        submenu.classList.add("header-menu__submenu-block--open")
        item.classList.add("header-menu__menu-item--active")
        updateSubmenuGap()
      }

      var hide = function () {
        hideTimeout = setTimeout(function () {
          submenu.classList.remove("header-menu__submenu-block--open")
          item.classList.remove("header-menu__menu-item--active")
        }, 150)
      }

      on(item, "mouseenter", show)
      on(item, "mouseleave", hide)
    })
  }

  /* =========================
   *  Мобильное меню
   * ========================= */

  function MobileMenu() {
    this.burgerBtn = qs(".mobile-menu__burger-btn")
    this.closeBtn = qs(".mobile-menu__close-btn")
    this.container = qs(".mobile-menu__container")
    this.menuList = qs(".mobile-menu__list", this.container || document)
    this.body = document.body

    if (!this.burgerBtn || !this.closeBtn || !this.container || !this.menuList) {
      return
    }

    this.isOpen = false
    this.init()
  }

  MobileMenu.prototype.init = function () {
    var self = this

    on(this.burgerBtn, "click", function (e) {
      e.preventDefault()
      self.open()
    })

    on(this.closeBtn, "click", function (e) {
      e.preventDefault()
      self.close()
    })

    on(this.container, "click", function (e) {
      if (e.target === self.container) {
        self.close()
      }
    })

    on(document, "keydown", function (e) {
      if (!self.isOpen) return
      if (e.key === "Escape" || e.key === "Esc" || e.keyCode === 27) {
        self.close()
      }
    })

    this.initSubmenus()
  }

  MobileMenu.prototype.open = function () {
    if (this.isOpen) return
    this.isOpen = true
    this.container.style.display = "block"
    this.body.classList.add("menu-open")
    this.container.style.opacity = "1"
    this.container.style.transform = "translateY(0)"
    this.closeAllSubmenus()
  }

  MobileMenu.prototype.close = function () {
    var self = this
    if (!this.isOpen) return

    this.isOpen = false
    this.container.style.opacity = "0"
    this.container.style.transform = "translateY(-10px)"
    this.closeAllSubmenus()

    setTimeout(function () {
      self.container.style.display = "none"
      self.body.classList.remove("menu-open")
      self.container.style.opacity = ""
      self.container.style.transform = ""
    }, 200)
  }

  MobileMenu.prototype.initSubmenus = function () {
    var self = this
    var dropdownItems = qsa(".mobile-menu__menu-item--dropdown", this.menuList)
    if (!dropdownItems.length) return

    dropdownItems.forEach(function (item) {
      var submenu = qs(".sub-menu", item)
      if (submenu) {
        submenu.style.maxHeight = "0"
        submenu.style.overflow = "hidden"
        submenu.style.opacity = "0"
      }

      var link = qs("a", item)
      if (!link) return

      on(link, "click", function (e) {
        if (submenu) {
          e.preventDefault()
          self.toggleSubmenu(item)
        }
      })
    })
  }

  MobileMenu.prototype.toggleSubmenu = function (activeItem) {
    var self = this
    var dropdownItems = qsa(".mobile-menu__menu-item--dropdown", this.menuList)

    dropdownItems.forEach(function (item) {
      var submenu = qs(".sub-menu", item)
      if (!submenu) return

      if (item === activeItem) {
        if (item.classList.contains("is-open")) {
          self.closeSubmenu(item, submenu)
        } else {
          self.openSubmenu(item, submenu)
        }
      } else {
        self.closeSubmenu(item, submenu)
      }
    })
  }

  MobileMenu.prototype.openSubmenu = function (item, submenu) {
    submenu.style.display = "flex"
    var fullHeight = submenu.scrollHeight
    submenu.style.maxHeight = fullHeight + "px"
    submenu.style.opacity = "1"
    item.classList.add("is-open")
  }

  MobileMenu.prototype.closeSubmenu = function (item, submenu) {
    submenu.style.maxHeight = "0"
    submenu.style.opacity = "0"
    item.classList.remove("is-open")
  }

  MobileMenu.prototype.closeAllSubmenus = function () {
    var self = this
    var dropdownItems = qsa(".mobile-menu__menu-item--dropdown", this.menuList)

    dropdownItems.forEach(function (item) {
      var submenu = qs(".sub-menu", item)
      if (!submenu) return
      self.closeSubmenu(item, submenu)
    })
  }

  function initMobileMenu() {
    new MobileMenu()
  }

  /* =========================
   *  Переводчик (translator)
   * ========================= */

  function initTranslatorDropdowns() {
    var translators = qsa(".translator")
    if (!translators.length) return

    translators.forEach(function (translator) {
      var button = qs(".translator__button", translator)
      var list = qs(".translator__list", translator)
      if (!button || !list) return

      button.setAttribute("aria-expanded", "false")

      function openList() {
        translator.classList.add("translator--open")
        button.setAttribute("aria-expanded", "true")
      }

      function closeList() {
        translator.classList.remove("translator--open")
        button.setAttribute("aria-expanded", "false")
      }

      function toggleList() {
        var isOpen = button.getAttribute("aria-expanded") === "true"
        if (isOpen) {
          closeList()
        } else {
          qsa(".translator.translator--open").forEach(function (other) {
            if (other === translator) return
            other.classList.remove("translator--open")
            var otherBtn = qs(".translator__button", other)
            if (otherBtn) {
              otherBtn.setAttribute("aria-expanded", "false")
            }
          })
          openList()
        }
      }

      on(button, "click", function (e) {
        e.stopPropagation()
        toggleList()
      })

      on(document, "click", function (e) {
        if (!translator.contains(e.target)) {
          closeList()
        }
      })

      on(document, "keydown", function (e) {
        if (e.key === "Escape" || e.key === "Esc" || e.keyCode === 27) {
          if (translator.classList.contains("translator--open")) {
            closeList()
          }
        }
      })
    })
  }

  /* =========================
   *  Инициализация
   * ========================= */

  function initAll() {
    initHeaderMenuHover()
    updateSubmenuGap()
    initMobileMenu()
    initTranslatorDropdowns()
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", initAll)
  } else {
    initAll()
  }

  window.addEventListener("resize", updateSubmenuGap)
})()
