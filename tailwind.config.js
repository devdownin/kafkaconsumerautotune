/**
 * Thème du tableau de bord.
 *
 * Reprend à l'identique la configuration qui était déclarée en ligne dans
 * fragments/head.html et interprétée par le CDN Tailwind à chaque chargement de
 * page. Elle est désormais appliquée une fois, à la construction.
 *
 * `content` doit couvrir tout ce qui produit du HTML : les classes absentes de
 * ces fichiers sont purgées. Le comportement des pages vit dans
 * `static/js/pages/`, et ces fichiers composent du balisage à coups de classes
 * littérales — d'où le glob sur `static/js/**`, sans lequel elles seraient
 * purgées et les éléments construits en JavaScript s'afficheraient sans style.
 */
module.exports = {
    darkMode: 'class',
    content: [
        './src/main/resources/templates/**/*.html',
        './src/main/resources/static/**/*.html',
        // notifications.js compose les toasts : ses classes doivent survivre à la purge.
        './src/main/resources/static/js/**/*.js'
    ],
    theme: {
        extend: {
            colors: {
                'primary': '#135bec',
                'primary-dark': '#0f49bd',
                'background-light': '#f6f6f8',
                'background-dark': '#0f172a'
            },
            fontFamily: {
                'display': ['Inter']
            },
            borderRadius: {
                DEFAULT: '0.25rem',
                lg: '0.5rem',
                xl: '0.75rem',
                full: '9999px'
            }
        }
    },
    plugins: [
        require('@tailwindcss/forms')
    ]
};
