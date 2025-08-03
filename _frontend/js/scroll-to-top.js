document.addEventListener("DOMContentLoaded", function() {
  var btn = document.getElementById("scrollToTopBtn");
  if (!btn) return;
  btn.onclick = function() {
    window.scrollTo({ top: 0, behavior: "smooth" });
  };
});


// Сделать site_name кликабельным как логотип
document.addEventListener("DOMContentLoaded", function () {
  const title = document.querySelector(".md-header__title");
  if (title) {
    title.style.cursor = "pointer";
    title.addEventListener("click", function () {
      window.location.href = "https://halltape.github.io/RoadmapPage/";
    });
  }
});
