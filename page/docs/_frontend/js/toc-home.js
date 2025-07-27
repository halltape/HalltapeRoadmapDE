document.addEventListener("DOMContentLoaded", function() {
    // Проверяем, что мы НЕ на главной странице
  if (
    window.location.pathname === "/" ||
    window.location.pathname === "/index.html"
  ) {
    return; // Не добавляем кнопку на главной
  }
  var toc = document.querySelector('.md-sidebar--secondary .md-nav');
  if (toc && !document.getElementById('toc-home-btn')) {
    var homeBtn = document.createElement('a');
    homeBtn.href = '/'; // путь к главной
    homeBtn.id = 'toc-home-btn';
    homeBtn.innerHTML = 'ВЕРНУТЬСЯ НАЗАД';
    toc.parentNode.insertBefore(homeBtn, toc);
  }
});