import type { Metadata } from 'next';
import './globals.css';

export const metadata: Metadata = {
  title: 'PulseGuard Analyst Console',
  description: 'Live fraud alert feed, drill-down, and disposition capture',
};

export default function RootLayout({ children }: { children: React.ReactNode }) {
  return (
    <html lang="en">
      <body>
        <div className="topbar">
          <h1>PulseGuard — Analyst Console</h1>
          <nav>
            <a href="/">Live Feed</a>
            <a href="/metrics">Metrics</a>
          </nav>
        </div>
        {children}
      </body>
    </html>
  );
}
