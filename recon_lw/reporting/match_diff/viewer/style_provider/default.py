from recon_lw.reporting.match_diff.viewer.style_provider.base import ErrorExamplesStyleProvider


class DefaultErrorExamplesStyleProvider(ErrorExamplesStyleProvider):
    def get_styles(self) -> str:
        return '''
        <style>
            .wrap-collabsible {
                margin-top: auto;
            }
            
            input[type='checkbox'] {
              display: none;
            }
            
            .lbl-toggle {
              display: block;
            
              font-weight: bold;
              font-family: monospace;
              font-size: 0.75rem;
              text-align: left;
            
              padding: 1rem;
              cursor: pointer;
            
              border-radius: 7px;
              transition: all 0.25s ease-out;
            }
            
            .lbl-toggle::before {
              content: ' ';
              display: inline-block;
            
              border-top: 5px solid transparent;
              border-bottom: 5px solid transparent;
              border-left: 5px solid currentColor;
              vertical-align: middle;
              margin-right: .7rem;
              transform: translateY(-2px);
            
              transition: transform .2s ease-out;
            }
            
            .toggle:checked + .lbl-toggle::before {
              transform: rotate(90deg) translateX(-3px); }
            
            .collapsible-content {
              max-height: 0px;
              overflow: hidden;
              transition: max-height .25s ease-in-out; 
              overflow-wrap: break-word;
            }
            
            .toggle:checked + .lbl-toggle + .collapsible-content {
              max-height: 10000vh;
            }
            
            .toggle:checked + .lbl-toggle {
              border-bottom-right-radius: 0;
              border-bottom-left-radius: 0;
            }
            
            .collapsible-content .content-inner {
              background: white;
              border-bottom-left-radius: 7px;
              border-bottom-right-radius: 7px;
              padding: .5rem 1rem;
            }
            
            #code {
                background-color: white;
                font-size: 0.75rem;
            }
            
        </style>
        '''