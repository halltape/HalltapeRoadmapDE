document.addEventListener("DOMContentLoaded", function() {
  var btn = document.getElementById("scrollToTopBtn");
  if (!btn) return;
  btn.onclick = function() {
    window.scrollTo({ top: 0, behavior: "smooth" });
  };
});