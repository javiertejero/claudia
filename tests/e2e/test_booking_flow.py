import re

from playwright.sync_api import Page, expect


def test_login_and_book_seat(page: Page, test_server: str):
    # Go to the homepage
    page.goto(test_server)

    # Wait for login screen
    expect(page.locator("#login-screen")).to_be_visible()

    # Fill in the form
    page.locator("#input-animal").fill("loba")
    page.locator("#input-adjetivo").fill("astuta")

    # Click enter
    page.locator("button:has-text('Entrar a la sala')").click()

    # Check if we get to the app screen
    expect(page.locator("#app-screen")).to_be_visible()

    # Wait for websocket to connect and update the banner
    # It should say "Selecciona tus butacas." since MAX_ACTIVE_USERS=5
    banner = page.locator("#status-banner")
    expect(banner).to_contain_text(re.compile(r"Selecciona tus butacas.|en cola"))

    # Wait for the seats to load (there should be some free seats)
    free_seat = page.locator(".seat.free").first
    expect(free_seat).to_be_visible(timeout=5000)

    # Click the free seat
    free_seat.click()

    # The seat should turn into my-seat
    my_seat = page.locator(".seat.my-seat").first
    expect(my_seat).to_be_visible(timeout=5000)

    # Click "Guardar Reserva"
    page.locator("#finalizar").click()

    # # It should redirect to /transferencia or /thanks depending on quota.
    # page.wait_for_url(re.compile(r"/(thanks|transferencia)"))

    # # Verify we are on the next screen
    # expect(page).to_have_url(re.compile(r"/(thanks|transferencia)"))
