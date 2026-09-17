document.addEventListener("DOMContentLoaded", function () {
  const currentUrl = window.location.pathname;
  const menuItems = document.querySelectorAll(".sidebar-button");

  menuItems.forEach((item) => {
    if (item.getAttribute("href") === currentUrl) {
      item.classList.add("active");
    }
    item.addEventListener("click", () => {
      menuItems.forEach((nav) => nav.classList.remove("active"));
      item.classList.add("active");
    });
  });
});
