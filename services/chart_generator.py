import matplotlib
matplotlib.use('Agg')  # Use non-interactive backend for serverless
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import base64
import io
from typing import List, Dict, Any, Optional
import logging
from scipy import stats

logger = logging.getLogger(__name__)

class ChartGenerator:
    """Handles chart generation and visualization"""
    
    def __init__(self):
        # Set matplotlib style for serverless environment
        plt.style.use('default')
        plt.rcParams['figure.figsize'] = (10, 6)
        plt.rcParams['figure.dpi'] = 80  # Lower DPI for smaller file size
    
    async def create_scatterplot_with_regression(
        self, 
        data: List[Dict[str, Any]], 
        x_col: str, 
        y_col: str,
        title: Optional[str] = None
    ) -> str:
        """Create a scatterplot with regression line and return as base64 string"""
        try:
            if not data:
                return self._create_error_chart("No data provided")
            
            df = pd.DataFrame(data)
            
            # Extract x and y data
            if x_col not in df.columns or y_col not in df.columns:
                return self._create_error_chart(f"Columns '{x_col}' or '{y_col}' not found")
            
            x_data = pd.to_numeric(df[x_col], errors='coerce')
            y_data = pd.to_numeric(df[y_col], errors='coerce')
            
            # Remove NaN values
            valid_mask = ~(pd.isna(x_data) | pd.isna(y_data))
            x_clean = x_data[valid_mask]
            y_clean = y_data[valid_mask]
            
            if len(x_clean) < 2:
                return self._create_error_chart("Insufficient valid data points")
            
            # Create the plot
            fig, ax = plt.subplots(figsize=(8, 6))
            
            # Scatterplot
            ax.scatter(x_clean, y_clean, alpha=0.6, s=30, color='blue', edgecolors='black', linewidth=0.5)
            
            # Regression line
            if len(x_clean) >= 2:
                try:
                    slope, intercept, r_value, p_value, std_err = stats.linregress(x_clean, y_clean)
                    line_x = np.array([x_clean.min(), x_clean.max()])
                    line_y = slope * line_x + intercept
                    ax.plot(line_x, line_y, color='red', linestyle='--', linewidth=2, 
                           label=f'Regression line (R² = {r_value**2:.3f})')
                    ax.legend()
                except Exception as e:
                    logger.warning(f"Could not fit regression line: {str(e)}")
            
            # Formatting
            ax.set_xlabel(x_col.replace('_', ' ').title())
            ax.set_ylabel(y_col.replace('_', ' ').title())
            ax.set_title(title or f'{y_col.title()} vs {x_col.title()}')
            ax.grid(True, alpha=0.3)
            
            plt.tight_layout()
            
            # Convert to base64
            return self._fig_to_base64(fig)
            
        except Exception as e:
            logger.error(f"Error creating scatterplot: {str(e)}")
            return self._create_error_chart(f"Error: {str(e)}")
    
    async def create_bar_chart(
        self,
        data: List[Dict[str, Any]],
        x_col: str,
        y_col: str,
        title: Optional[str] = None
    ) -> str:
        """Create a bar chart and return as base64 string"""
        try:
            if not data:
                return self._create_error_chart("No data provided")
            
            df = pd.DataFrame(data)
            
            if x_col not in df.columns or y_col not in df.columns:
                return self._create_error_chart(f"Columns '{x_col}' or '{y_col}' not found")
            
            # Create the plot
            fig, ax = plt.subplots(figsize=(10, 6))
            
            # Convert y to numeric
            y_data = pd.to_numeric(df[y_col], errors='coerce')
            
            # Create bar chart
            bars = ax.bar(df[x_col], y_data, color='skyblue', edgecolor='black', linewidth=0.5)
            
            # Formatting
            ax.set_xlabel(x_col.replace('_', ' ').title())
            ax.set_ylabel(y_col.replace('_', ' ').title())
            ax.set_title(title or f'{y_col.title()} by {x_col.title()}')
            
            # Rotate x-axis labels if needed
            if len(df) > 10:
                plt.xticks(rotation=45, ha='right')
            
            ax.grid(True, alpha=0.3, axis='y')
            plt.tight_layout()
            
            return self._fig_to_base64(fig)
            
        except Exception as e:
            logger.error(f"Error creating bar chart: {str(e)}")
            return self._create_error_chart(f"Error: {str(e)}")
    
    async def create_line_chart(
        self,
        data: List[Dict[str, Any]],
        x_col: str,
        y_col: str,
        title: Optional[str] = None
    ) -> str:
        """Create a line chart and return as base64 string"""
        try:
            if not data:
                return self._create_error_chart("No data provided")
            
            df = pd.DataFrame(data)
            
            if x_col not in df.columns or y_col not in df.columns:
                return self._create_error_chart(f"Columns '{x_col}' or '{y_col}' not found")
            
            # Convert to numeric
            x_data = pd.to_numeric(df[x_col], errors='coerce')
            y_data = pd.to_numeric(df[y_col], errors='coerce')
            
            # Sort by x values
            sorted_data = pd.DataFrame({x_col: x_data, y_col: y_data}).dropna().sort_values(x_col)
            
            # Create the plot
            fig, ax = plt.subplots(figsize=(10, 6))
            
            ax.plot(sorted_data[x_col], sorted_data[y_col], marker='o', linewidth=2, markersize=4, color='blue')
            
            # Formatting
            ax.set_xlabel(x_col.replace('_', ' ').title())
            ax.set_ylabel(y_col.replace('_', ' ').title())
            ax.set_title(title or f'{y_col.title()} vs {x_col.title()}')
            ax.grid(True, alpha=0.3)
            
            plt.tight_layout()
            
            return self._fig_to_base64(fig)
            
        except Exception as e:
            logger.error(f"Error creating line chart: {str(e)}")
            return self._create_error_chart(f"Error: {str(e)}")
    
    def _fig_to_base64(self, fig) -> str:
        """Convert matplotlib figure to base64 string"""
        try:
            buffer = io.BytesIO()
            
            # Save with optimization for size
            fig.savefig(buffer, format='png', dpi=60, bbox_inches='tight', 
                       facecolor='white', edgecolor='none', optimize=True)
            buffer.seek(0)
            
            # Check size and optimize if needed
            img_size = buffer.getbuffer().nbytes
            if img_size > 100000:  # 100KB limit
                # Try lower DPI
                buffer = io.BytesIO()
                fig.savefig(buffer, format='png', dpi=40, bbox_inches='tight',
                           facecolor='white', edgecolor='none', optimize=True)
                buffer.seek(0)
            
            img_base64 = base64.b64encode(buffer.read()).decode('utf-8')
            plt.close(fig)  # Important: free memory
            
            return f"data:image/png;base64,{img_base64}"
            
        except Exception as e:
            logger.error(f"Error converting figure to base64: {str(e)}")
            plt.close(fig)
            return self._create_minimal_error_image()
    
    def _create_error_chart(self, error_message: str) -> str:
        """Create a simple error message chart"""
        try:
            fig, ax = plt.subplots(figsize=(6, 4))
            ax.text(0.5, 0.5, f"Error: {error_message}", 
                   horizontalalignment='center', verticalalignment='center',
                   transform=ax.transAxes, fontsize=12, color='red',
                   bbox=dict(boxstyle="round,pad=0.3", facecolor="yellow", alpha=0.5))
            ax.set_xlim(0, 1)
            ax.set_ylim(0, 1)
            ax.axis('off')
            
            return self._fig_to_base64(fig)
            
        except Exception:
            return self._create_minimal_error_image()
    
    def _create_minimal_error_image(self) -> str:
        """Return a minimal 1x1 pixel error image"""
        return "data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mNk+M9QDwADhgGAWjR9awAAAABJRU5ErkJggg=="