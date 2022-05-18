<!-- Global site tag (gtag.js) - Google Analytics -->
<script async src="https://www.googletagmanager.com/gtag/js?id=G-6X8Z5N2CGR"></script>
<script>
  window.dataLayer = window.dataLayer || [];
  function gtag(){dataLayer.push(arguments);}
  gtag('js', new Date());

  gtag('config', 'G-6X8Z5N2CGR');
</script>
<script>
(function(i,s,o,g,r,a,m){i['GoogleAnalyticsObject']=r;i[r]=i[r]||function(){
(i[r].q=i[r].q||[]).push(arguments)},i[r].l=1*new Date();a=s.createElement(o),
m=s.getElementsByTagName(o)[0];a.async=1;a.src=g;m.parentNode.insertBefore(a,m)
})(window,document,'script','https://www.google-analytics.com/analytics.js','ga');
// Creates an adblock detection plugin.
ga('provide', 'adblockTracker', function(tracker, opts) {
var ad = document.createElement('ins');
ad.className = 'AdSense'; //Name of ad partner you're working with.
ad.style.display = 'block';
ad.style.position = 'absolute';
ad.style.top = '-1px';
ad.style.height = '1px';
document.body.appendChild(ad);
tracker.set('dimension' + opts.dimensionIndex, !ad.clientHeight);
document.body.removeChild(ad);
});
ga('create', 'G-6X8Z5N2CGR', 'auto'); //Your tracking ID.
ga('require', 'adblockTracker', {dimensionIndex: 1});
ga('send', 'pageview');
</script>

# Добро пожаловать на портал документации Picodata
Picodata  — это распределенный сервер приложений со встроенной распределенной базой данных. Этот продукт предоставляет систему хранения данных и платформу для работы персистентных приложений на языке программирования Rust.
Подробнее о данном программном продукте вы можете узнать на сайте [picodata.io](https://www.picodata.io).

## Доступные документы и статьи

* Общее [описание](description) продукта
* [Преимущества](benefits) использования Picodata
* [Преимущества языка Rust](benefits_rust) при разработке приложений
* [Установка и первые шаги](install)
* Изменение [версий приложений и схемы данных](arch)

<a style="display: none" href="https://hits.seeyoufarm.com"><img src="https://hits.seeyoufarm.com/api/count/incr/badge.svg?url=https%3A%2F%2Fdocs.picodata.io%2Fpicodata%2F&count_bg=%2379C83D&title_bg=%23555555&icon=&icon_color=%23E7E7E7&title=hits&edge_flat=false"/></a>