// Web Component: <ticket-card>
// Encapsulates a responsive, highly-styled ticket card inspired by retro cinema tickets.

class TicketCard extends HTMLElement {
    constructor() {
        super();
        this.attachShadow({ mode: 'open' });
    }

    static get observedAttributes() {
        return ['user-id', 'session-time', 'fila', 'butaca'];
    }

    attributeChangedCallback() {
        this.render();
    }

    connectedCallback() {
        this.render();
    }



    // Helper: Determine background color depending on session time
    getBgColor(sessionTime) {
        if (!sessionTime) return '#f6f2e8';
        const norm = sessionTime.toLowerCase().replace(/\s+/g, '');
        if (norm.includes('11')) return '#cac9d9'; // 11h
        if (norm.includes('12:45') || norm.includes('12.45')) return '#d4b3a2'; // 12:45h
        if (norm.includes('18')) return '#87a3a6'; // 18h
        return '#f6f2e8'; // Fallback paper cream
    }

    render() {
        const userId = this.getAttribute('user-id') || '';
        const sessionTime = this.getAttribute('session-time') || 'N/A';
        const fila = this.getAttribute('fila') || '-';
        const butaca = this.getAttribute('butaca') || '-';

        const bgColor = this.getBgColor(sessionTime);

        // Update the custom property on the host for clean styling access
        this.style.setProperty('--ticket-bg', bgColor);

        this.shadowRoot.innerHTML = `
            <style>
                @import url('https://fonts.googleapis.com/css2?family=Caveat:wght@700&family=Montserrat:ital,wght@0,300;0,400;0,700;0,900;1,300&display=swap');

                :host {
                    display: block;
                    container-type: inline-size;
                    width: 100%;
                    max-width: 580px;
                    margin: 15px auto;
                    box-sizing: border-box;
                }

                .ticket {
                    background-color: var(--ticket-bg, #f6f2e8);
                    color: #2b2b2b;
                    position: relative;
                    box-shadow: 0 8px 24px rgba(0, 0, 0, 0.4), 0 2px 6px rgba(0, 0, 0, 0.2);
                    padding: 24px;
                    display: flex;
                    flex-direction: column;
                    align-items: center;
                    border-radius: 12px;
                    overflow: hidden;
                    box-sizing: border-box;
                    font-family: 'Montserrat', sans-serif;
                    
                    /* Textured background */
                    background-image: 
                        linear-gradient(90deg, rgba(0,0,0,0.02) 1px, transparent 1px),
                        linear-gradient(rgba(0,0,0,0.015) 1px, transparent 1px);
                    background-size: 4px 4px;
                    transition: background-color 0.3s ease;
                }

                /* Cut line for vertical */
                .perforation {
                    position: absolute;
                    top: 45px;
                    left: 0;
                    right: 0;
                    height: 2px;
                    background-image: linear-gradient(to right, #b5af9e 60%, transparent 40%);
                    background-size: 8px 2px;
                }

                .ticket-header {
                    font-size: 1rem;
                    font-weight: 700;
                    letter-spacing: 6px;
                    margin-top: -5px;
                    margin-bottom: 30px;
                    color: rgba(43,43,43,0.5);
                    text-transform: uppercase;
                }

                .ticket-body {
                    width: 100%;
                    display: flex;
                    flex-direction: column;
                    align-items: center;
                    gap: 16px;
                }

                .stub-section {
                    display: flex;
                    flex-direction: column;
                    align-items: center;
                    width: 100%;
                }

                .logo-container {
                    display: flex;
                    justify-content: center;
                    margin-bottom: 8px;
                }

                .ticket-logo {
                    height: 44px;
                    width: auto;
                    object-fit: contain;
                }

                .brand-name {
                    font-size: 2.2rem;
                    font-weight: 400;
                    margin: 0;
                    letter-spacing: -1px;
                    line-height: 1.1;
                    text-align: center;
                }

                .badge-torrellenc {
                    background-color: #2b2b2b;
                    color: var(--ticket-bg, #f6f2e8);
                    font-size: 0.75rem;
                    font-weight: 900;
                    letter-spacing: 4px;
                    padding: 4px 20px;
                    margin-top: 8px;
                    text-transform: uppercase;
                    text-align: center;
                    width: 80%;
                    transition: color 0.3s ease;
                }

                .founded-text {
                    font-size: 0.75rem;
                    font-style: italic;
                    font-weight: 300;
                    margin-top: 4px;
                    margin-bottom: 10px;
                }

                .main-section {
                    display: flex;
                    flex-direction: column;
                    align-items: center;
                    width: 100%;
                }

                .top-info-row {
                    display: flex;
                    flex-direction: row;
                    justify-content: center;
                    align-items: center;
                    gap: 20px;
                    width: 100%;
                    margin-bottom: 15px;
                }

                .general-section {
                    display: flex;
                    align-items: center;
                    gap: 8px;
                }

                .label-print {
                    font-size: 0.8rem;
                    font-weight: 700;
                    letter-spacing: 1px;
                    text-transform: uppercase;
                }

                .checkbox-box {
                    width: 20px;
                    height: 20px;
                    border: 2px solid #2b2b2b;
                    display: flex;
                    justify-content: center;
                    align-items: center;
                    position: relative;
                }

                .handwritten-ink {
                    font-family: 'Caveat', cursive;
                    color: #123399; /* Blue ink */
                    font-size: 1.8rem;
                    line-height: 0.8;
                    transform: rotate(-2deg);
                    display: inline-block;
                }



                .session-data {
                    width: 100%;
                    display: flex;
                    flex-direction: column;
                    gap: 8px;
                }

                .data-row {
                    display: flex;
                    align-items: flex-end;
                    justify-content: space-between;
                    height: 34px;
                }

                .underline-row {
                    border-bottom: 1px solid #2b2b2b;
                    padding-bottom: 2px;
                }

                .font-large {
                    font-size: 2.2rem;
                    padding-right: 8px;
                }

                /* HORIZONTAL LAYOUT */
                @container (min-width: 480px) {
                    .ticket {
                        flex-direction: row;
                        height: 240px;
                        padding: 0;
                        align-items: stretch;
                    }

                    .perforation {
                        display: none;
                    }

                    .ticket-header {
                        writing-mode: vertical-rl;
                        transform: rotate(180deg);
                        margin: 0;
                        padding: 0 12px;
                        font-size: 0.85rem;
                        letter-spacing: 4px;
                        border-right: 1px solid rgba(0,0,0,0.05);
                        display: flex;
                        align-items: center;
                        justify-content: center;
                        background-color: rgba(0,0,0,0.02);
                    }

                    .ticket-body {
                        flex-direction: row;
                        align-items: center;
                        flex: 1;
                        padding: 16px 20px;
                        gap: 20px;
                    }

                    .stub-section {
                        width: 180px;
                        flex-shrink: 0;
                        justify-content: center;
                        height: 100%;
                        border-right: none;
                    }

                    .main-section {
                        flex: 1;
                        height: 100%;
                        justify-content: space-between;
                    }

                    .brand-name {
                        font-size: 1.8rem;
                    }

                    .badge-torrellenc {
                        width: 90%;
                        font-size: 0.7rem;
                        margin-top: 6px;
                    }

                    .founded-text {
                        margin-bottom: 0;
                        font-size: 0.7rem;
                    }

                    .top-info-row {
                        justify-content: flex-start;
                        margin-bottom: 10px;
                    }

                    .session-data {
                        display: grid;
                        grid-template-columns: 1.2fr 1fr 1fr;
                        gap: 12px;
                        width: 100%;
                        align-items: end;
                    }

                    .data-row {
                        flex-direction: column;
                        align-items: flex-start;
                        justify-content: flex-end;
                        height: auto;
                        gap: 2px;
                    }

                    .underline-row {
                        border-bottom: none;
                        padding-bottom: 0;
                    }

                    .font-large {
                        font-size: 2.2rem;
                    }
                }
            </style>
            <div class="ticket">
                <div class="perforation"></div>
                <div class="ticket-header">ENTRADA</div>
                <div class="ticket-body">
                    <div class="stub-section">
                        <div class="logo-container">
                            <img class="ticket-logo" src="/static/torrelles-portes-optimized.webp" alt="Logo">
                        </div>
                        <h1 class="brand-name">l'Ateneu</h1>
                        <div class="badge-torrellenc">TORRELLENC</div>
                        <div class="founded-text">des de 1929</div>
                    </div>
                    <div class="main-section">
                        <div class="top-info-row">
                            <div class="general-section">
                                <span class="label-print">GENERAL</span>
                                <div class="checkbox-box">
                                    <span class="handwritten-ink">X</span>
                                </div>
                            </div>
                        </div>
                        <div class="session-data">
                            <div class="data-row">
                                <span class="label-print">Sessió:</span>
                                <span class="handwritten-ink">${sessionTime}</span>
                            </div>
                            <div class="data-row underline-row">
                                <span class="label-print">Fila</span>
                                <span class="handwritten-ink font-large">${fila}</span>
                            </div>
                            <div class="data-row">
                                <span class="label-print">Seient</span>
                                <span class="handwritten-ink font-large">${butaca}</span>
                            </div>
                        </div>
                    </div>
                </div>
            </div>
        `;
    }
}

customElements.define('ticket-card', TicketCard);
